// Package engine defines the narrow key-value engine contract the ledger stores
// rely on, so that the engine (Pebble today, RocksDB under evaluation) can be
// swapped below it. The surface is deliberately the subset of the Pebble API
// the repository actually uses: bounded iterators, point lookups with a
// resource closer, write batches with sync/no-sync commit, snapshots and
// checkpoints.
//
// Semantics follow Pebble, which every caller was written against:
//
//   - Get returns engine.ErrNotFound for a missing key. The returned bytes are
//     valid until the returned io.Closer is closed.
//   - Iterator.Key / Value are valid until the next positioning call.
//   - SeekLT positions on the last key strictly less than the argument.
//   - Running off either end leaves the iterator invalid but positioned
//     "before first" / "after last": the opposite step (Next after Prev
//     exhausted, Prev after Next exhausted) re-enters at that edge.
//   - IterOptions bounds are [LowerBound, UpperBound).
//   - Batch.Commit(sync=false) is durable only after a later sync (Pebble
//     NoSync); Commit(sync=true) fsyncs the WAL.
//   - Checkpoint produces a consistent, openable copy of the DB in dir
//     without flushing the memtable (Pebble WithFlushedWAL): the WAL is
//     synced and copied.
//
// See docs/drafts/rocksdb-poc.md.
package engine

import (
	"errors"
	"io"
)

// ErrNotFound is returned by Get when the key does not exist.
var ErrNotFound = errors.New("kv: key not found")

// IterOptions bounds an iterator to [LowerBound, UpperBound). A nil bound is
// unbounded on that side.
type IterOptions struct {
	LowerBound []byte
	UpperBound []byte
}

// Iterator is a positioned cursor over an ordered key space. Every method
// mirrors *pebble.Iterator.
type Iterator interface {
	SeekGE(key []byte) bool
	// SeekPrefixGE seeks like SeekGE and then confines iteration to keys
	// sharing key's prefix (Pebble Comparer.Split / RocksDB prefix
	// extractor; the whole key when no prefix is configured) until the
	// next absolute positioning call. Engines with a prefix bloom use it to
	// skip files that cannot contain the prefix.
	SeekPrefixGE(key []byte) bool
	SeekLT(key []byte) bool
	First() bool
	Last() bool
	Next() bool
	Prev() bool
	Valid() bool
	Key() []byte
	Value() []byte
	ValueAndErr() ([]byte, error)
	Error() error
	Close() error
}

// Getter is the point-lookup capability.
type Getter interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

// Reader is the full read capability: point lookups and bounded iteration.
type Reader interface {
	Getter
	NewIter(opts *IterOptions) (Iterator, error)
}

// Snapshot is a pinned, point-in-time Reader. Close releases the pin;
// iterators opened on it must be closed first.
type Snapshot interface {
	Reader
	Close() error
}

// Batch accumulates writes applied atomically by Commit. Close releases the
// batch without applying it (or after a successful Commit).
type Batch interface {
	Set(key, value []byte) error
	Delete(key []byte) error
	SingleDelete(key []byte) error
	DeleteRange(start, end []byte) error
	// Count reports the number of operations recorded so far.
	Count() uint32
	// Len reports the encoded size of the batch in bytes.
	Len() int
	Commit(sync bool) error
	Close() error
}

// DB is an open engine instance.
type DB interface {
	Reader
	NewSnapshot() Snapshot
	NewBatch() Batch
	// Set writes a single key with the given durability.
	Set(key, value []byte, sync bool) error
	// SyncWAL forces every write committed so far to be durable. Pebble
	// implements it as LogData(nil, Sync).
	SyncWAL() error
	// Checkpoint writes an openable copy of the database to dir, which must
	// not exist.
	Checkpoint(dir string) error
	Flush() error
	// Compact compacts the key range [start, end].
	Compact(start, end []byte) error
	// Stats returns engine-neutral size counters. Engine-specific metrics
	// stay behind the concrete type.
	Stats() Stats
	Close() error
}

// Stats is the engine-neutral subset of storage metrics.
type Stats struct {
	// Levels lists file count and byte size per LSM level, L0 first.
	Levels []LevelStats
	// MemTableBytes is the size of the active and immutable memtables.
	MemTableBytes uint64
	// DiskSpaceBytes is the total on-disk footprint (SSTs, WAL, manifests).
	DiskSpaceBytes uint64
	// BlockCacheHits and BlockCacheMisses are cumulative.
	BlockCacheHits   uint64
	BlockCacheMisses uint64
}

// LevelStats is the per-level entry of Stats.Levels.
type LevelStats struct {
	Files int64
	Bytes uint64
}

// Options is the engine-neutral open configuration. Engines map it onto
// their own tuning knobs and may accept extra engine-specific settings
// through their own constructors.
type Options struct {
	// CacheBytes sizes the block cache. Zero uses the engine default.
	CacheBytes int64
	// MemTableBytes sizes the write buffer. Zero uses the engine default.
	MemTableBytes int64
	// BloomBitsPerKey configures the per-SST bloom filter. Zero disables it.
	BloomBitsPerKey int
	// FixedPrefixLen enables a fixed-length prefix bloom (Pebble Split /
	// RocksDB prefix extractor). Zero disables it.
	FixedPrefixLen int
	// MaxConcurrentCompactions bounds background compaction parallelism.
	MaxConcurrentCompactions int
	// ReadOnly opens the database without taking the write lock.
	ReadOnly bool
	// ErrorIfExists refuses to open an existing database.
	ErrorIfExists bool
}
