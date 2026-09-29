//go:build rocksdb

// Package rockskv implements engine on RocksDB through grocksdb. It is only
// compiled with the "rocksdb" build tag so that default builds stay pure Go
// (CGO_ENABLED=0); see docs/drafts/rocksdb-poc.md.
package rocksengine

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"strconv"

	"github.com/linxGnu/grocksdb"

	"github.com/formancehq/ledger/v3/internal/storage/engine"
)

// numLevels mirrors RocksDB's default num_levels.
const numLevels = 7

// DB wraps *grocksdb.DB together with the option objects RocksDB keeps
// referencing for the lifetime of the database.
type DB struct {
	db       *grocksdb.DB
	opts     *grocksdb.Options
	readOnly bool
	// prefixLen is the fixed prefix extractor length (0: whole key), used
	// to emulate Pebble's SeekPrefixGE confinement.
	prefixLen int
	syncWO    *grocksdb.WriteOptions
	noSyncWO  *grocksdb.WriteOptions
	plainRO   *grocksdb.ReadOptions
	cache     *grocksdb.Cache
}

var _ engine.DB = (*DB)(nil)

// Open opens or creates a RocksDB database at dir.
func Open(dir string, o engine.Options) (*DB, error) {
	// Pebble creates the directory (and parents) on open; RocksDB only
	// creates the leaf.
	if !o.ReadOnly {
		if err := os.MkdirAll(dir, 0o750); err != nil {
			return nil, fmt.Errorf("rocksdb open: creating directory: %w", err)
		}
	}
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(!o.ErrorIfExists || true)
	opts.SetErrorIfExists(o.ErrorIfExists)
	// Match Pebble's level shape: dynamic level sizing off keeps L1 as the
	// base level, which is what the Pebble metrics and tuning assume.
	opts.SetLevelCompactionDynamicLevelBytes(false)
	opts.SetNumLevels(numLevels)
	if o.MemTableBytes > 0 {
		opts.SetWriteBufferSize(uint64(o.MemTableBytes))
	}
	if o.MaxConcurrentCompactions > 0 {
		opts.SetMaxBackgroundJobs(o.MaxConcurrentCompactions + 1)
	}

	bbto := grocksdb.NewDefaultBlockBasedTableOptions()
	var cache *grocksdb.Cache
	if o.CacheBytes > 0 {
		cache = grocksdb.NewLRUCache(uint64(o.CacheBytes))
		bbto.SetBlockCache(cache)
	}
	if o.BloomBitsPerKey > 0 {
		bbto.SetFilterPolicy(grocksdb.NewBloomFilter(float64(o.BloomBitsPerKey)))
		bbto.SetWholeKeyFiltering(true)
	}
	opts.SetBlockBasedTableFactory(bbto)
	if o.FixedPrefixLen > 0 {
		// Ownership passes to the options object; never destroy it here.
		opts.SetPrefixExtractor(grocksdb.NewFixedPrefixTransform(o.FixedPrefixLen))
		opts.SetMemTablePrefixBloomSizeRatio(0.1)
	}

	var (
		db  *grocksdb.DB
		err error
	)
	if o.ReadOnly {
		db, err = grocksdb.OpenDbForReadOnly(opts, dir, false)
	} else {
		db, err = grocksdb.OpenDb(opts, dir)
	}
	if err != nil {
		if cache != nil {
			cache.Destroy()
		}
		if o.FixedPrefixLen == 0 {
			opts.Destroy()
		}

		return nil, mapOpenError(err)
	}

	syncWO := grocksdb.NewDefaultWriteOptions()
	syncWO.SetSync(true)
	noSyncWO := grocksdb.NewDefaultWriteOptions()
	noSyncWO.SetSync(false)

	return &DB{
		db:        db,
		opts:      opts,
		readOnly:  o.ReadOnly,
		prefixLen: o.FixedPrefixLen,
		syncWO:    syncWO,
		noSyncWO:  noSyncWO,
		plainRO:   grocksdb.NewDefaultReadOptions(),
		cache:     cache,
	}, nil
}

func mapOpenError(err error) error {
	return fmt.Errorf("rocksdb open: %w", err)
}

// Raw exposes the underlying handle for engine-specific paths.
func (d *DB) Raw() *grocksdb.DB { return d.db }

func (d *DB) Get(key []byte) ([]byte, io.Closer, error) {
	return getWith(d.db, d.plainRO, key)
}

func (d *DB) NewIter(opts *engine.IterOptions) (engine.Iterator, error) {
	return newIter(d.db, nil, opts, d.prefixLen)
}

func (d *DB) NewSnapshot() engine.Snapshot {
	return &snapshot{db: d.db, snap: d.db.NewSnapshot(), prefixLen: d.prefixLen}
}

func (d *DB) NewBatch() engine.Batch {
	return &batch{db: d, buf: make([]byte, batchHeaderLen, 4096)}
}

func (d *DB) Set(key, value []byte, sync bool) error {
	if d.readOnly {
		return errors.New("rocksdb: database opened read-only")
	}

	return d.db.Put(d.writeOptions(sync), key, value)
}

func (d *DB) SyncWAL() error { return d.db.FlushWAL(true) }

// Checkpoint mirrors Pebble's WithFlushedWAL: RocksDB flushes the memtable
// when the WAL is larger than logSizeForFlush, so a MaxUint64 threshold
// keeps the memtable in place and copies the (synced) WAL instead.
func (d *DB) Checkpoint(dir string) error {
	if _, err := os.Stat(dir); err == nil {
		return fmt.Errorf("rocksdb checkpoint: destination %q already exists", dir)
	}
	// Pebble creates missing parents; RocksDB does not.
	if err := os.MkdirAll(filepath.Dir(dir), 0o750); err != nil {
		return fmt.Errorf("rocksdb checkpoint: creating parent directory: %w", err)
	}
	cp, err := d.db.NewCheckpoint()
	if err != nil {
		return err
	}
	defer cp.Destroy()

	return cp.CreateCheckpoint(dir, math.MaxUint64)
}

func (d *DB) Flush() error {
	fo := grocksdb.NewDefaultFlushOptions()
	defer fo.Destroy()

	return d.db.Flush(fo)
}

func (d *DB) Compact(start, end []byte) error {
	d.db.CompactRange(grocksdb.Range{Start: start, Limit: end})

	return nil
}

func (d *DB) Stats() engine.Stats {
	s := engine.Stats{Levels: make([]engine.LevelStats, numLevels)}
	for i := range numLevels {
		// num-files-at-levelN is a string property, not an int property.
		files, _ := strconv.ParseInt(d.db.GetProperty("rocksdb.num-files-at-level"+strconv.Itoa(i)), 10, 64)
		s.Levels[i] = engine.LevelStats{Files: files}
	}
	// Per-level bytes come from the levelstats table.
	sst, _ := d.db.GetIntProperty("rocksdb.total-sst-files-size")
	s.DiskSpaceBytes = sst
	if len(s.Levels) > 0 {
		s.Levels[len(s.Levels)-1].Bytes = sst // approximation: total, not per level
	}
	s.MemTableBytes, _ = d.db.GetIntProperty("rocksdb.cur-size-all-mem-tables")
	if d.opts != nil {
		s.BlockCacheHits = tickerByName(d.opts.GetStatisticsString(), "rocksdb.block.cache.hit")
		s.BlockCacheMisses = tickerByName(d.opts.GetStatisticsString(), "rocksdb.block.cache.miss")
	}

	return s
}

// tickerByName parses a ticker from the statistics dump; the grocksdb
// TickerType enum drifts across RocksDB versions (see the POC spike).
func tickerByName(dump, name string) uint64 {
	marker := name + " COUNT : "
	i := bytes.Index([]byte(dump), []byte(marker))
	if i < 0 {
		return 0
	}
	rest := dump[i+len(marker):]
	end := 0
	for end < len(rest) && rest[end] >= '0' && rest[end] <= '9' {
		end++
	}
	v, _ := strconv.ParseUint(rest[:end], 10, 64)

	return v
}

func (d *DB) Close() error {
	d.db.Close()
	d.syncWO.Destroy()
	d.noSyncWO.Destroy()
	d.plainRO.Destroy()
	if d.cache != nil {
		d.cache.Destroy()
	}
	// Options.Destroy double-frees objects RocksDB took ownership of
	// (prefix extractor, merge operator); the options object is leaked on
	// purpose when one is set. See docs/drafts/rocksdb-poc.md.

	return nil
}

func (d *DB) writeOptions(sync bool) *grocksdb.WriteOptions {
	if sync {
		return d.syncWO
	}

	return d.noSyncWO
}

// --- point lookups ---

// sliceCloser frees the C allocation backing a Get result.
type sliceCloser struct{ s *grocksdb.Slice }

func (c sliceCloser) Close() error {
	c.s.Free()

	return nil
}

func getWith(db *grocksdb.DB, ro *grocksdb.ReadOptions, key []byte) ([]byte, io.Closer, error) {
	s, err := db.Get(ro, key)
	if err != nil {
		return nil, nil, err
	}
	if !s.Exists() {
		s.Free()

		return nil, nil, engine.ErrNotFound
	}

	return s.Data(), sliceCloser{s: s}, nil
}

// --- snapshots ---

type snapshot struct {
	db        *grocksdb.DB
	snap      *grocksdb.Snapshot
	ro        *grocksdb.ReadOptions
	prefixLen int
}

func (s *snapshot) readOptions() *grocksdb.ReadOptions {
	if s.ro == nil {
		s.ro = grocksdb.NewDefaultReadOptions()
		s.ro.SetSnapshot(s.snap)
	}

	return s.ro
}

func (s *snapshot) Get(key []byte) ([]byte, io.Closer, error) {
	return getWith(s.db, s.readOptions(), key)
}

func (s *snapshot) NewIter(opts *engine.IterOptions) (engine.Iterator, error) {
	return newIter(s.db, s.snap, opts, s.prefixLen)
}

func (s *snapshot) Close() error {
	if s.ro != nil {
		s.ro.Destroy()
	}
	s.db.ReleaseSnapshot(s.snap)

	return nil
}

// --- iterators ---

type iterator struct {
	it        *grocksdb.Iterator
	ro        *grocksdb.ReadOptions
	lower     []byte
	upper     []byte
	prefixLen int
	// prefix is non-nil while confined by SeekPrefixGE.
	prefix []byte
}

func newIter(db *grocksdb.DB, snap *grocksdb.Snapshot, opts *engine.IterOptions, prefixLen int) (engine.Iterator, error) {
	ro := grocksdb.NewDefaultReadOptions()
	if snap != nil {
		ro.SetSnapshot(snap)
	}
	it := &iterator{ro: ro, prefixLen: prefixLen}
	if opts != nil {
		// Copy: RocksDB keeps referencing the bound bytes for the lifetime of
		// the read options, and callers commonly reuse key buffers.
		if opts.LowerBound != nil {
			it.lower = bytes.Clone(opts.LowerBound)
			ro.SetIterateLowerBound(it.lower)
		}
		if opts.UpperBound != nil {
			it.upper = bytes.Clone(opts.UpperBound)
			ro.SetIterateUpperBound(it.upper)
		}
	}
	it.it = db.NewIterator(ro)

	return it, nil
}

func (it *iterator) SeekGE(key []byte) bool {
	it.prefix = nil
	it.it.Seek(key)

	return it.Valid()
}

// SeekPrefixGE confines the iterator to key's prefix. RocksDB decides
// prefix mode at iterator creation (prefix_same_as_start), so the
// confinement is enforced here on Valid(); the prefix bloom still applies
// to the underlying Seek when an extractor is configured.
func (it *iterator) SeekPrefixGE(key []byte) bool {
	n := len(key)
	if it.prefixLen > 0 && it.prefixLen < n {
		n = it.prefixLen
	}
	it.prefix = bytes.Clone(key[:n])
	it.it.Seek(key)

	return it.Valid()
}

// SeekLT positions on the last key strictly below key. RocksDB's SeekForPrev
// is "last key <= key", so an exact hit steps back once.
func (it *iterator) SeekLT(key []byte) bool {
	it.prefix = nil
	it.it.SeekForPrev(key)
	if it.it.Valid() && bytes.Equal(it.it.KeySlice().Data(), key) {
		it.it.Prev()
	}

	return it.Valid()
}

func (it *iterator) First() bool {
	it.prefix = nil
	it.it.SeekToFirst()

	return it.Valid()
}

func (it *iterator) Last() bool {
	it.prefix = nil
	it.it.SeekToLast()

	return it.Valid()
}

func (it *iterator) Next() bool {
	it.it.Next()

	return it.Valid()
}

func (it *iterator) Prev() bool {
	it.it.Prev()

	return it.Valid()
}

func (it *iterator) Valid() bool {
	if !it.it.Valid() {
		return false
	}
	if it.prefix != nil {
		return bytes.HasPrefix(it.it.KeySlice().Data(), it.prefix)
	}

	return true
}

func (it *iterator) Key() []byte { return it.it.KeySlice().Data() }

func (it *iterator) Value() []byte { return it.it.ValueSlice().Data() }

// ValueAndErr never reports an error here: RocksDB surfaces iteration
// errors through Error() once the iterator turns invalid, and polling
// rocksdb_iter_get_error per key costs a cgo call plus a heap escape.
func (it *iterator) ValueAndErr() ([]byte, error) { return it.Value(), nil }

func (it *iterator) Error() error { return it.it.Err() }

func (it *iterator) Close() error {
	it.it.Close()
	it.ro.Destroy()

	return nil
}

// --- batches ---

// RocksDB write batch wire format (db/write_batch.cc): an 8-byte sequence
// number (0 until applied), a little-endian uint32 record count, then one
// record per operation: [tag][varint32 len][key]([varint32 len][value]).
// Encoding it in Go turns N cgo calls per batch into one at commit
// (rocksdb_writebatch_create_from); the FSM apply path issues many small
// Sets per entry and the per-call overhead dominated the write benchmarks.
const (
	batchHeaderLen = 12

	tagDeletion       byte = 0x0
	tagValue          byte = 0x1
	tagSingleDeletion byte = 0x7
	tagRangeDeletion  byte = 0xF
)

type batch struct {
	db    *DB
	buf   []byte
	count uint32
}

func (b *batch) appendSlice(v []byte) {
	b.buf = binary.AppendUvarint(b.buf, uint64(len(v)))
	b.buf = append(b.buf, v...)
}

func (b *batch) Set(key, value []byte) error {
	if b.buf == nil {
		return errors.New("rocksdb: batch closed")
	}
	b.buf = append(b.buf, tagValue)
	b.appendSlice(key)
	b.appendSlice(value)
	b.count++

	return nil
}

func (b *batch) Delete(key []byte) error {
	if b.buf == nil {
		return errors.New("rocksdb: batch closed")
	}
	b.buf = append(b.buf, tagDeletion)
	b.appendSlice(key)
	b.count++

	return nil
}

func (b *batch) SingleDelete(key []byte) error {
	if b.buf == nil {
		return errors.New("rocksdb: batch closed")
	}
	b.buf = append(b.buf, tagSingleDeletion)
	b.appendSlice(key)
	b.count++

	return nil
}

func (b *batch) DeleteRange(start, end []byte) error {
	if b.buf == nil {
		return errors.New("rocksdb: batch closed")
	}
	b.buf = append(b.buf, tagRangeDeletion)
	b.appendSlice(start)
	b.appendSlice(end)
	b.count++

	return nil
}

func (b *batch) Count() uint32 { return b.count }

func (b *batch) Len() int { return len(b.buf) }

func (b *batch) Commit(sync bool) error {
	if b.db.readOnly {
		return errors.New("rocksdb: database opened read-only")
	}
	if b.buf == nil {
		return errors.New("rocksdb: batch closed")
	}
	binary.LittleEndian.PutUint32(b.buf[8:12], b.count)
	wb := grocksdb.WriteBatchFrom(b.buf)
	defer wb.Destroy()

	return b.db.db.Write(b.db.writeOptions(sync), wb)
}

func (b *batch) Close() error {
	b.buf = nil

	return nil
}
