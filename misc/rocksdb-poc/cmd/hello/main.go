// Command hello validates the cgo + grocksdb build chain for the RocksDB POC.
// It opens a database, writes a batch, reads it back through a bounded
// iterator and creates a checkpoint — the primitives the ledger main store
// relies on. See docs/drafts/rocksdb-poc.md.
package main

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/linxGnu/grocksdb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

func run() error {
	dir, err := os.MkdirTemp("", "rocksdb-hello-*")
	if err != nil {
		return err
	}
	defer os.RemoveAll(dir)

	opts := grocksdb.NewDefaultOptions()
	defer opts.Destroy()
	opts.SetCreateIfMissing(true)

	db, err := grocksdb.OpenDb(opts, filepath.Join(dir, "db"))
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer db.Close()

	wo := grocksdb.NewDefaultWriteOptions()
	defer wo.Destroy()
	wo.SetSync(false)

	batch := grocksdb.NewWriteBatch()
	defer batch.Destroy()
	for i := range 10 {
		batch.Put([]byte(fmt.Sprintf("k/%02d", i)), []byte(fmt.Sprintf("v%d", i)))
	}
	if err := db.Write(wo, batch); err != nil {
		return fmt.Errorf("write: %w", err)
	}

	ro := grocksdb.NewDefaultReadOptions()
	defer ro.Destroy()
	ro.SetIterateLowerBound([]byte("k/03"))
	ro.SetIterateUpperBound([]byte("k/07"))

	it := db.NewIterator(ro)
	defer it.Close()
	var got [][]byte
	for it.SeekToFirst(); it.Valid(); it.Next() {
		k := it.Key()
		got = append(got, bytes.Clone(k.Data()))
		k.Free()
	}
	if err := it.Err(); err != nil {
		return fmt.Errorf("iter: %w", err)
	}
	if len(got) != 4 || string(got[0]) != "k/03" || string(got[3]) != "k/06" {
		return fmt.Errorf("unexpected bounded scan result: %q", got)
	}

	missing, err := db.Get(ro, []byte("nope"))
	if err != nil {
		return fmt.Errorf("get: %w", err)
	}
	if missing.Exists() {
		return fmt.Errorf("expected missing key")
	}
	missing.Free()

	cp, err := db.NewCheckpoint()
	if err != nil {
		return fmt.Errorf("checkpoint object: %w", err)
	}
	defer cp.Destroy()
	cpDir := filepath.Join(dir, "cp")
	if err := cp.CreateCheckpoint(cpDir, 0); err != nil {
		return fmt.Errorf("create checkpoint: %w", err)
	}
	entries, err := os.ReadDir(cpDir)
	if err != nil {
		return err
	}

	fmt.Printf("rocksdb ok: bounded scan=%d keys, checkpoint=%d files\n", len(got), len(entries))

	return nil
}
