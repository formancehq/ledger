// Package kv defines the small storage contract used by Ledger's RocksDB stores.
// Values returned by Get are owned by Go; iterator keys and values are borrowed
// until the next movement or Close.
package kv

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/linxGnu/grocksdb"
)

var ErrNotFound = errors.New("key not found")

type Options struct {
	ReadOnly       bool
	DisableWAL     bool
	CacheSize      uint64
	ComparatorName string
	MergeOperator  grocksdb.MergeOperator
	Configure      func(*grocksdb.Options)
}

type DB struct {
	raw           *grocksdb.DB
	options       *grocksdb.Options
	mergeMu       sync.Mutex
	mergeOperator grocksdb.MergeOperator
	disableWAL    bool
	read          *grocksdb.ReadOptions
	write         *grocksdb.WriteOptions
	syncWrite     *grocksdb.WriteOptions
	cache         *grocksdb.Cache
	table         *grocksdb.BlockBasedTableOptions
}

func Open(path string, config Options) (*DB, error) {
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(!config.ReadOnly)
	if config.ComparatorName != "" {
		opts.SetComparator(grocksdb.NewComparator(config.ComparatorName, bytes.Compare))
	}
	var cache *grocksdb.Cache
	var table *grocksdb.BlockBasedTableOptions
	if config.CacheSize > 0 {
		cache = grocksdb.NewLRUCache(config.CacheSize)
		table = grocksdb.NewDefaultBlockBasedTableOptions()
		table.SetBlockCache(cache)
		table.SetFilterPolicy(grocksdb.NewBloomFilterFull(10))
		opts.SetBlockBasedTableFactory(table)
	}
	if config.Configure != nil {
		config.Configure(opts)
	}
	var (
		raw *grocksdb.DB
		err error
	)
	if config.ReadOnly {
		raw, err = grocksdb.OpenDbForReadOnly(opts, path, false)
	} else {
		raw, err = grocksdb.OpenDb(opts, path)
	}
	if err != nil {
		opts.Destroy()
		if table != nil {
			table.Destroy()
		}
		if cache != nil {
			cache.Destroy()
		}
		return nil, err
	}
	write := grocksdb.NewDefaultWriteOptions()
	syncWrite := grocksdb.NewDefaultWriteOptions()
	syncWrite.SetSync(true)
	if config.DisableWAL {
		write.DisableWAL(true)
		syncWrite.DisableWAL(true)
	}
	return &DB{raw: raw, options: opts, read: grocksdb.NewDefaultReadOptions(), write: write, syncWrite: syncWrite, cache: cache, table: table, mergeOperator: config.MergeOperator, disableWAL: config.DisableWAL}, nil
}

func (d *DB) Raw() *grocksdb.DB { return d.raw }

func (d *DB) Close() error {
	d.read.Destroy()
	d.write.Destroy()
	d.syncWrite.Destroy()
	d.raw.Close()
	d.options.Destroy()
	if d.table != nil {
		d.table.Destroy()
	}
	if d.cache != nil {
		d.cache.Destroy()
	}
	return nil
}

type nopCloser struct{}

func (nopCloser) Close() error { return nil }

func (d *DB) Get(key []byte) ([]byte, io.Closer, error) {
	value, err := d.raw.GetBytes(d.read, key)
	if err != nil {
		return nil, nil, err
	}
	if value == nil {
		return nil, nil, ErrNotFound
	}
	return value, nopCloser{}, nil
}

type WriteOptions struct{ Sync bool }

var (
	NoSync = &WriteOptions{}
	Sync   = &WriteOptions{Sync: true}
)

func (d *DB) Set(key, value []byte, options *WriteOptions) error {
	return d.raw.Put(d.writeOptions(options), key, value)
}

func (d *DB) Delete(key []byte, options *WriteOptions) error {
	return d.raw.Delete(d.writeOptions(options), key)
}

func (d *DB) SingleDelete(key []byte, options *WriteOptions) error {
	return d.raw.SingleDelete(d.writeOptions(options), key)
}

func (d *DB) Merge(key, value []byte, options *WriteOptions) error {
	if d.mergeOperator != nil {
		d.mergeMu.Lock()
		defer d.mergeMu.Unlock()
		base, err := d.raw.GetBytes(d.read, key)
		if err != nil {
			return err
		}
		merged, ok := d.mergeOperator.FullMerge(key, base, [][]byte{value})
		if !ok {
			return fmt.Errorf("merge operator %s rejected key %x", d.mergeOperator.Name(), key)
		}
		return d.raw.Put(d.writeOptions(options), key, merged)
	}
	return d.raw.Merge(d.writeOptions(options), key, value)
}

func (d *DB) writeOptions(options *WriteOptions) *grocksdb.WriteOptions {
	if options != nil && options.Sync {
		return d.syncWrite
	}
	return d.write
}

type Batch struct {
	db     *DB
	raw    *grocksdb.WriteBatch
	closed bool
}

func (d *DB) NewBatch() *Batch                                  { return &Batch{db: d, raw: grocksdb.NewWriteBatch()} }
func (b *Batch) Set(key, value []byte, _ *WriteOptions) error   { b.raw.Put(key, value); return nil }
func (b *Batch) Delete(key []byte, _ *WriteOptions) error       { b.raw.Delete(key); return nil }
func (b *Batch) SingleDelete(key []byte, _ *WriteOptions) error { b.raw.SingleDelete(key); return nil }
func (b *Batch) DeleteRange(lower, upper []byte, _ *WriteOptions) error {
	b.raw.DeleteRange(lower, upper)
	return nil
}
func (b *Batch) Commit(options *WriteOptions) error {
	return b.db.raw.Write(b.db.writeOptions(options), b.raw)
}
func (b *Batch) Close() error {
	if !b.closed {
		b.raw.Destroy()
		b.closed = true
	}
	return nil
}

type IterOptions struct{ LowerBound, UpperBound []byte }

type Iterator struct {
	raw    *grocksdb.Iterator
	opts   *grocksdb.ReadOptions
	prefix []byte
}

func (d *DB) NewIter(bounds *IterOptions) (*Iterator, error) {
	return newIter(d.raw, nil, bounds), nil
}

func newIter(db *grocksdb.DB, snapshot *grocksdb.Snapshot, bounds *IterOptions) *Iterator {
	opts := grocksdb.NewDefaultReadOptions()
	if snapshot != nil {
		opts.SetSnapshot(snapshot)
	}
	if bounds != nil {
		if bounds.LowerBound != nil {
			opts.SetIterateLowerBound(bytes.Clone(bounds.LowerBound))
		}
		if bounds.UpperBound != nil {
			opts.SetIterateUpperBound(bytes.Clone(bounds.UpperBound))
		}
	}
	return &Iterator{raw: db.NewIterator(opts), opts: opts}
}

func (i *Iterator) valid() bool {
	if !i.raw.Valid() {
		return false
	}
	return i.prefix == nil || bytes.HasPrefix(i.Key(), i.prefix)
}
func (i *Iterator) First() bool            { i.prefix = nil; i.raw.SeekToFirst(); return i.valid() }
func (i *Iterator) Last() bool             { i.prefix = nil; i.raw.SeekToLast(); return i.valid() }
func (i *Iterator) Next() bool             { i.raw.Next(); return i.valid() }
func (i *Iterator) Prev() bool             { i.raw.Prev(); return i.valid() }
func (i *Iterator) SeekGE(key []byte) bool { i.prefix = nil; i.raw.Seek(key); return i.valid() }
func (i *Iterator) SeekPrefixGE(prefix, key []byte) bool {
	i.prefix = bytes.Clone(prefix)
	i.raw.Seek(key)
	return i.valid()
}
func (i *Iterator) SeekLT(key []byte) bool {
	i.prefix = nil
	i.raw.SeekForPrev(key)
	if i.raw.Valid() && bytes.Equal(i.Key(), key) {
		i.raw.Prev()
	}
	return i.valid()
}
func (i *Iterator) Valid() bool { return i.valid() }
func (i *Iterator) Key() []byte {
	if value := i.raw.Key(); value != nil {
		return value.Data()
	}
	return nil
}
func (i *Iterator) Value() []byte {
	if value := i.raw.Value(); value != nil {
		return value.Data()
	}
	return nil
}
func (i *Iterator) ValueAndErr() ([]byte, error) { return i.Value(), i.Error() }
func (i *Iterator) Error() error                 { return i.raw.Err() }
func (i *Iterator) Close() error                 { i.raw.Close(); i.opts.Destroy(); return nil }

type Snapshot struct {
	db  *DB
	raw *grocksdb.Snapshot
}

func (d *DB) NewSnapshot() *Snapshot { return &Snapshot{db: d, raw: d.raw.NewSnapshot()} }
func (s *Snapshot) Get(key []byte) ([]byte, io.Closer, error) {
	opts := grocksdb.NewDefaultReadOptions()
	defer opts.Destroy()
	opts.SetSnapshot(s.raw)
	value, err := s.db.raw.GetBytes(opts, key)
	if err != nil {
		return nil, nil, err
	}
	if value == nil {
		return nil, nil, ErrNotFound
	}
	return value, nopCloser{}, nil
}
func (s *Snapshot) NewIter(bounds *IterOptions) (*Iterator, error) {
	return newIter(s.db.raw, s.raw, bounds), nil
}
func (s *Snapshot) Close() error { s.db.raw.ReleaseSnapshot(s.raw); return nil }

func (d *DB) Checkpoint(dest string) error {
	if !d.disableWAL {
		// The C checkpoint API with a large log-size threshold did not
		// preserve committed unsynced writes in the DAL tests. Materialize
		// an SST before creating primary-store checkpoints.
		if err := d.Flush(); err != nil {
			return err
		}
		if err := d.SyncWAL(); err != nil {
			return err
		}
	}
	cp, err := d.raw.NewCheckpoint()
	if err != nil {
		return err
	}
	defer cp.Destroy()
	// Callers with WAL disabled flush explicitly before a published checkpoint.
	// Avoid forcing a flush here: a direct checkpoint is also used to model a
	// sudden process death before unflushed derived writes reach an SST.
	return cp.CreateCheckpoint(dest, math.MaxUint64)
}

func (d *DB) Flush() error {
	opts := grocksdb.NewDefaultFlushOptions()
	defer opts.Destroy()
	opts.SetWait(true)
	return d.raw.Flush(opts)
}

func (d *DB) SyncWAL() error { return d.raw.FlushWAL(true) }

func (d *DB) Compact(ctx context.Context, start, end []byte, _ bool) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	d.raw.CompactRange(grocksdb.Range{Start: start, Limit: end})
	return ctx.Err()
}

func (d *DB) Property(name string) string { return d.raw.GetProperty(name) }
