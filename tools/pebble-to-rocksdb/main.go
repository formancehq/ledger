package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"flag"
	"fmt"
	"hash"
	"os"
	"path/filepath"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/linxGnu/grocksdb"
)

const (
	readComparerName  = "formance.readstore.v2"
	usageComparerName = "formance.usagestore.v1"
	batchLimit        = 4 << 20
)

type config struct {
	source, destination, kind string
	stopped                   bool
}

func main() {
	var c config
	flag.StringVar(&c.source, "source", "", "stopped Pebble database directory or immutable snapshot")
	flag.StringVar(&c.destination, "destination", "", "new RocksDB database directory")
	flag.StringVar(&c.kind, "kind", "", "main, read, or usage")
	flag.BoolVar(&c.stopped, "stopped", false, "acknowledge Ledger is stopped or source is an immutable snapshot")
	flag.Parse()
	if flag.NArg() != 0 {
		fmt.Fprintln(os.Stderr, "unexpected positional arguments")
		os.Exit(2)
	}
	if err := migrate(c); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func migrate(c config) error {
	if !c.stopped || c.source == "" || c.destination == "" {
		return errors.New("require --stopped, --source, and --destination; stop Ledger or use an immutable source snapshot")
	}
	cmp, err := sourceComparer(c.kind)
	if err != nil {
		return err
	}
	source, err := filepath.Abs(c.source)
	if err != nil {
		return err
	}
	dest, err := filepath.Abs(c.destination)
	if err != nil {
		return err
	}
	if err := safePaths(source, dest); err != nil {
		return err
	}
	stage := dest + ".incomplete"
	if err := os.Mkdir(stage, 0o700); err != nil {
		return fmt.Errorf("create staging directory %s (existing incomplete state must be inspected and moved aside manually): %w", stage, err)
	}
	// Deliberately leave staging on every failure for inspection. Never touch source.
	src, err := pebble.Open(source, &pebble.Options{ReadOnly: true, Comparer: cmp})
	if err != nil {
		return fmt.Errorf("open Pebble source: %w", err)
	}
	copyErr := copyAndVerify(src, source, stage, c.kind)
	closeErr := src.Close()
	if copyErr != nil {
		return fmt.Errorf("migration incomplete at %s: %w", stage, errors.Join(copyErr, closeErr))
	}
	if closeErr != nil {
		return fmt.Errorf("close Pebble source before publication; staging remains at %s: %w", stage, closeErr)
	}
	if err := syncDir(stage); err != nil {
		return fmt.Errorf("sync staging directory: %w", err)
	}
	// Same-parent rename is atomic. The destination is never replaced.
	if err := publishExclusive(stage, dest); err != nil {
		return fmt.Errorf("publish verified database: %w", err)
	}
	if err := syncDir(filepath.Dir(dest)); err != nil {
		return fmt.Errorf("destination published at %s but parent sync failed; inspect it before use: %w", dest, err)
	}
	fmt.Printf("published verified RocksDB at %s\n", dest)
	return nil
}

func sourceComparer(kind string) (*pebble.Comparer, error) {
	if kind == "main" {
		return pebble.DefaultComparer, nil
	}
	var name string
	switch kind {
	case "read":
		name = readComparerName
	case "usage":
		name = usageComparerName
	default:
		return nil, fmt.Errorf("invalid --kind %q: choose main, read, or usage", kind)
	}
	// Both Ledger comparers use bytewise ordering. Preserve their persisted name
	// and bloom-prefix split when opening the old SSTables.
	cmp := *pebble.DefaultComparer
	cmp.Name = name
	cmp.Split = func(key []byte) int {
		if len(key) <= 1 || key[0] == 0xFE /* internal singleton prefix */ {
			return len(key)
		}
		if len(key) >= 65 {
			return 65
		}
		return len(key)
	}
	cmp.ImmediateSuccessor = func(dst, prefix []byte) []byte {
		dst = append(dst[:0], prefix...)
		if len(dst) == 65 {
			dst[64]++
			return dst
		}
		return append(dst, 0)
	}
	return &cmp, nil
}

func safePaths(source, dest string) error {
	info, err := os.Stat(source)
	if err != nil || !info.IsDir() {
		return fmt.Errorf("source must be an existing directory: %s", source)
	}
	parent, err := filepath.EvalSymlinks(filepath.Dir(dest))
	if err != nil {
		return fmt.Errorf("destination parent must exist: %w", err)
	}
	realSource, err := filepath.EvalSymlinks(source)
	if err != nil {
		return err
	}
	realDest := filepath.Join(parent, filepath.Base(dest))
	if realDest == realSource || strings.HasPrefix(realDest, realSource+string(os.PathSeparator)) || strings.HasPrefix(realSource, realDest+string(os.PathSeparator)) {
		return errors.New("source and destination must be separate directories")
	}
	for _, path := range []string{dest, dest + ".incomplete"} {
		_, err := os.Lstat(path)
		if err == nil {
			return fmt.Errorf("refusing existing destination or staging path: %s", path)
		}
		if !os.IsNotExist(err) {
			return err
		}
	}
	return nil
}

type digest struct {
	count uint64
	hash  [32]byte
}

type streamHash struct {
	h     hash.Hash
	count uint64
}

func newStreamHash() *streamHash { return &streamHash{h: sha256.New()} }

func (s *streamHash) add(key, value []byte) {
	var length [8]byte
	binary.BigEndian.PutUint64(length[:], uint64(len(key)))
	s.h.Write(length[:])
	s.h.Write(key)
	binary.BigEndian.PutUint64(length[:], uint64(len(value)))
	s.h.Write(length[:])
	s.h.Write(value)
	s.count++
}

func (s *streamHash) result() digest {
	var result digest
	result.count = s.count
	copy(result.hash[:], s.h.Sum(nil))
	return result
}

func copyAndVerify(src *pebble.DB, source, stage, kind string) error {
	opts := grocksdb.NewDefaultOptions()
	defer opts.Destroy()
	opts.SetCreateIfMissing(true)
	if kind != "main" {
		name := readComparerName
		if kind == "usage" {
			name = usageComparerName
		}
		cmp := grocksdb.NewComparator(name, bytes.Compare)
		defer cmp.Destroy()
		opts.SetComparator(cmp)
	}
	db, err := grocksdb.OpenDb(opts, stage)
	if err != nil {
		return fmt.Errorf("open RocksDB staging: %w", err)
	}
	writeOpts := grocksdb.NewDefaultWriteOptions()
	writeOpts.SetSync(true)
	defer writeOpts.Destroy()
	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	iter, err := src.NewIter(nil)
	if err != nil {
		db.Close()
		return err
	}
	stream := newStreamHash()
	var batchBytes int
	for iter.First(); iter.Valid(); iter.Next() {
		key, value := iter.Key(), iter.Value()
		batch.Put(key, value)
		stream.add(key, value)
		batchBytes += len(key) + len(value)
		if batchBytes >= batchLimit {
			if err := db.Write(writeOpts, batch); err != nil {
				_ = iter.Close()
				db.Close()
				return err
			}
			batch.Clear()
			batchBytes = 0
		}
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		db.Close()
		return err
	}
	if err := iter.Close(); err != nil {
		db.Close()
		return err
	}
	if batch.Count() > 0 {
		if err := db.Write(writeOpts, batch); err != nil {
			db.Close()
			return err
		}
	}
	flush := grocksdb.NewDefaultFlushOptions()
	flush.SetWait(true)
	err = db.Flush(flush)
	flush.Destroy()
	db.Close()
	if err != nil {
		return err
	}
	want := stream.result()
	got, err := hashRocks(stage, kind)
	if err != nil {
		return err
	}
	if got != want {
		return fmt.Errorf("key/value mismatch: source count=%d sha256=%x, destination count=%d sha256=%x", want.count, want.hash, got.count, got.hash)
	}
	// Detect an accidentally moving source before publication.
	current, err := hashPebble(src)
	if err != nil {
		return err
	}
	if current != want {
		return errors.New("source changed during migration; discard staging after inspection and retry from a stopped source or snapshot")
	}
	if err := copyReadyMarker(source, stage); err != nil {
		return err
	}
	fmt.Printf("verified %d key/value pairs, sha256=%x\n", got.count, got.hash)
	return nil
}

// A ready query checkpoint remains ready only after its converted database
// has passed the full key/value verification above.
func copyReadyMarker(source, stage string) error {
	oldPath := filepath.Join(source, ".ready")
	info, err := os.Lstat(oldPath)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("query checkpoint marker is not a regular file: %s", oldPath)
	}
	data, err := os.ReadFile(oldPath)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(filepath.Join(stage, ".ready"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o640)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

func hashPebble(db *pebble.DB) (digest, error) {
	iter, err := db.NewIter(nil)
	if err != nil {
		return digest{}, err
	}
	s := newStreamHash()
	for iter.First(); iter.Valid(); iter.Next() {
		s.add(iter.Key(), iter.Value())
	}
	if err := iter.Error(); err != nil {
		_ = iter.Close()
		return digest{}, err
	}
	if err := iter.Close(); err != nil {
		return digest{}, err
	}
	return s.result(), nil
}

func hashRocks(path, kind string) (digest, error) {
	opts := grocksdb.NewDefaultOptions()
	defer opts.Destroy()
	if kind != "main" {
		name := readComparerName
		if kind == "usage" {
			name = usageComparerName
		}
		cmp := grocksdb.NewComparator(name, bytes.Compare)
		defer cmp.Destroy()
		opts.SetComparator(cmp)
	}
	db, err := grocksdb.OpenDbForReadOnly(opts, path, false)
	if err != nil {
		return digest{}, err
	}
	defer db.Close()
	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	iter := db.NewIterator(ro)
	defer iter.Close()
	s := newStreamHash()
	for iter.SeekToFirst(); iter.Valid(); iter.Next() {
		key, value := iter.Key(), iter.Value()
		s.add(key.Data(), value.Data())
		key.Free()
		value.Free()
	}
	if err := iter.Err(); err != nil {
		return digest{}, err
	}
	return s.result(), nil
}

func syncDir(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	if err := f.Sync(); err != nil {
		return err
	}
	return nil
}
