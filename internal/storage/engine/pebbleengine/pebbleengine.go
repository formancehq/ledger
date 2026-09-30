// Package pebblekv implements engine on CockroachDB Pebble.
package pebbleengine

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/bloom"

	"github.com/formancehq/ledger/v3/internal/storage/engine"
)

// DB wraps *pebble.DB. The underlying handle stays reachable through Pebble
// so engine-specific paths (metrics, event listeners) keep working during the
// migration.
type DB struct {
	*pebble.DB
}

var _ engine.DB = (*DB)(nil)

// Open opens or creates a Pebble database at dir.
func Open(dir string, o engine.Options) (*DB, error) {
	opts := &pebble.Options{
		FormatMajorVersion: pebble.FormatNewest,
		ReadOnly:           o.ReadOnly,
		ErrorIfExists:      o.ErrorIfExists,
		Logger:             quietLogger{},
	}
	if o.CacheBytes > 0 {
		opts.Cache = pebble.NewCache(o.CacheBytes)
		defer opts.Cache.Unref()
	}
	if o.MemTableBytes > 0 {
		opts.MemTableSize = uint64(o.MemTableBytes)
	}
	if o.MaxConcurrentCompactions > 0 {
		n := o.MaxConcurrentCompactions
		opts.CompactionConcurrencyRange = func() (int, int) { return 1, n }
	}
	if o.BloomBitsPerKey > 0 {
		for i := range opts.Levels {
			opts.Levels[i].FilterPolicy = bloom.FilterPolicy(o.BloomBitsPerKey)
			opts.Levels[i].FilterType = pebble.TableFilter
		}
	}
	if o.FixedPrefixLen > 0 {
		opts.Comparer = fixedPrefixComparer(o.FixedPrefixLen)
	}

	db, err := pebble.Open(dir, opts)
	if err != nil {
		return nil, err
	}

	return &DB{DB: db}, nil
}

// Wrap adapts an already-open *pebble.DB.
func Wrap(db *pebble.DB) *DB { return &DB{DB: db} }

// quietLogger drops Pebble's informational output; fatal errors still panic
// through the default behaviour.
type quietLogger struct{}

func (quietLogger) Infof(string, ...any)  {}
func (quietLogger) Errorf(string, ...any) {}
func (quietLogger) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}

// fixedPrefixComparer keeps default ordering and splits keys at n bytes for
// prefix bloom filters, mirroring RocksDB's fixed prefix extractor.
func fixedPrefixComparer(n int) *pebble.Comparer {
	c := *pebble.DefaultComparer
	c.Name = fmt.Sprintf("engine.fixedprefix.%d", n)
	c.Split = func(key []byte) int {
		if len(key) < n {
			return len(key)
		}

		return n
	}

	return &c
}

func (d *DB) Get(key []byte) ([]byte, io.Closer, error) {
	return mapGet(d.DB.Get(key))
}

func (d *DB) NewIter(opts *engine.IterOptions) (engine.Iterator, error) {
	return newIter(d.DB, opts)
}

func (d *DB) NewSnapshot() engine.Snapshot { return &snapshot{Snapshot: d.DB.NewSnapshot()} }

func (d *DB) NewBatch() engine.Batch { return &batch{Batch: d.DB.NewBatch()} }

func (d *DB) Set(key, value []byte, sync bool) error {
	return d.DB.Set(key, value, writeOptions(sync))
}

func (d *DB) SyncWAL() error { return d.LogData(nil, pebble.Sync) }

func (d *DB) Checkpoint(dir string) error {
	return d.DB.Checkpoint(dir, pebble.WithFlushedWAL())
}

func (d *DB) Flush() error { return d.DB.Flush() }

func (d *DB) Compact(start, end []byte) error {
	return d.DB.Compact(context.Background(), start, end, false)
}

func (d *DB) Stats() engine.Stats {
	m := d.Metrics()
	s := engine.Stats{
		Levels:           make([]engine.LevelStats, len(m.Levels)),
		MemTableBytes:    m.MemTable.Size,
		DiskSpaceBytes:   m.DiskSpaceUsage(),
		BlockCacheHits:   uint64(max(m.BlockCache.Hits, 0)),
		BlockCacheMisses: uint64(max(m.BlockCache.Misses, 0)),
	}
	for i, l := range m.Levels {
		s.Levels[i] = engine.LevelStats{Files: l.TablesCount, Bytes: uint64(max(l.TablesSize, 0))}
	}

	return s
}

func (d *DB) Close() error { return d.DB.Close() }

type snapshot struct {
	*pebble.Snapshot
}

func (s *snapshot) Get(key []byte) ([]byte, io.Closer, error) { return mapGet(s.Snapshot.Get(key)) }

func (s *snapshot) NewIter(opts *engine.IterOptions) (engine.Iterator, error) {
	return newIter(s.Snapshot, opts)
}

func (s *snapshot) Close() error { return s.Snapshot.Close() }

type pebbleReader interface {
	NewIter(o *pebble.IterOptions) (*pebble.Iterator, error)
}

func newIter(r pebbleReader, opts *engine.IterOptions) (engine.Iterator, error) {
	var po *pebble.IterOptions
	if opts != nil {
		po = &pebble.IterOptions{LowerBound: opts.LowerBound, UpperBound: opts.UpperBound}
	}
	it, err := r.NewIter(po)
	if err != nil {
		return nil, err
	}

	return &iterator{Iterator: it}, nil
}

type iterator struct {
	*pebble.Iterator
}

func (it *iterator) SeekGE(key []byte) bool { return it.Iterator.SeekGE(key) }

func (it *iterator) SeekPrefixGE(key []byte) bool { return it.Iterator.SeekPrefixGE(key) }

func (it *iterator) SeekLT(key []byte) bool { return it.Iterator.SeekLT(key) }

func (it *iterator) Close() error { return it.Iterator.Close() }

func mapGet(val []byte, closer io.Closer, err error) ([]byte, io.Closer, error) {
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, engine.ErrNotFound
		}

		return nil, nil, err
	}

	return val, closer, nil
}

type batch struct {
	*pebble.Batch
}

func (b *batch) Set(key, value []byte) error { return b.Batch.Set(key, value, nil) }

func (b *batch) Delete(key []byte) error { return b.Batch.Delete(key, nil) }

func (b *batch) SingleDelete(key []byte) error { return b.Batch.SingleDelete(key, nil) }

func (b *batch) DeleteRange(start, end []byte) error { return b.Batch.DeleteRange(start, end, nil) }

func (b *batch) Count() uint32 { return b.Batch.Count() }

func (b *batch) Len() int { return b.Batch.Len() }

func (b *batch) Commit(sync bool) error { return b.Batch.Commit(writeOptions(sync)) }

func (b *batch) Close() error { return b.Batch.Close() }

func writeOptions(sync bool) *pebble.WriteOptions {
	if sync {
		return pebble.Sync
	}

	return pebble.NoSync
}
