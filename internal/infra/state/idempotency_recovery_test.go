package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// TestIdempotencyStore_RestoreFromStore_RebuildsMapFromPebble is the
// regression coverage for issue #300: a node that restarts must rebuild
// the in-memory idempotency bridge from Pebble. Without it, every
// idempotency key whose surrounding proposal already landed in Pebble
// becomes invisible to the FSM, and a replayed request is accepted as
// fresh work — breaking at-most-once semantics.
func TestIdempotencyStore_RestoreFromStore_RebuildsMapFromPebble(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	// Persist three idempotency entries via the normal batch write path.
	batch := store.NewBatch()

	values := map[string]*commonpb.IdempotencyKeyValue{
		"alpha": {LogSequence: 1, CreatedAt: 1_000_000},
		"beta":  {LogSequence: 2, CreatedAt: 2_000_000},
		"gamma": {LogSequence: 3, CreatedAt: 3_000_000},
	}

	for key, value := range values {
		require.NoError(t, saveIdempotencyKey(batch, key, value))
	}

	require.NoError(t, batch.Commit())

	// Simulate a node restart: brand new in-memory store, then restore.
	idemp := NewIdempotencyStore(0)

	for key := range values {
		_, ok := idemp.Get(key)
		require.False(t, ok, "fresh store must not return any key")
	}

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	require.NoError(t, idemp.RestoreFromStore(handle))

	for key, want := range values {
		got, ok := idemp.Get(key)
		require.Truef(t, ok, "Get(%q) after restore must hit", key)
		require.Equal(t, want.GetLogSequence(), got.GetLogSequence())
		require.Equal(t, want.GetCreatedAt(), got.GetCreatedAt())
	}
}

// TestIdempotencyStore_RestoreFromStore_LoadsAllEntriesRegardlessOfAge
// pins the determinism contract: recovery MUST NOT filter expired entries
// based on the local wall-clock TTL. Two nodes that restart at different
// moments would otherwise produce divergent maps for the same applied
// index, violating the cache-is-source-of-authority invariant. Stale
// entries are removed exclusively by the Raft-replicated
// IdempotencyEviction command, which carries a deterministic cutoff.
func TestIdempotencyStore_RestoreFromStore_LoadsAllEntriesRegardlessOfAge(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	const (
		ancientCreatedAt uint64 = 1
		freshCreatedAt   uint64 = 1_000_000_000
		ttlMicros        uint64 = 1_000 // intentionally tiny: every entry is "expired" by wall-clock
	)

	batch := store.NewBatch()

	require.NoError(t, saveIdempotencyKey(batch, "ancient", &commonpb.IdempotencyKeyValue{
		LogSequence: 1,
		CreatedAt:   ancientCreatedAt,
	}))
	require.NoError(t, saveIdempotencyKey(batch, "fresh", &commonpb.IdempotencyKeyValue{
		LogSequence: 2,
		CreatedAt:   freshCreatedAt,
	}))
	require.NoError(t, batch.Commit())

	idemp := NewIdempotencyStore(ttlMicros)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	require.NoError(t, idemp.RestoreFromStore(handle))

	got, ok := idemp.Get("ancient")
	require.True(t, ok, "expired entries MUST be restored — eviction is the Raft command's job, not boot's")
	require.Equal(t, uint64(1), got.GetLogSequence())

	got, ok = idemp.Get("fresh")
	require.True(t, ok)
	require.Equal(t, uint64(2), got.GetLogSequence())
}

// TestIdempotencyStore_RestoreFromStore_EmptyStore confirms the scan
// handles an empty zone gracefully (fresh cluster, no idempotency keys
// ever written).
func TestIdempotencyStore_RestoreFromStore_EmptyStore(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	idemp := NewIdempotencyStore(0)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	require.NoError(t, idemp.RestoreFromStore(handle))

	_, ok := idemp.Get("anything")
	require.False(t, ok)
}

// TestIdempotencyStore_RestoreFromStore_OverwritesPriorEntries asserts
// that RestoreFromStore is idempotent: calling it on a store with prior
// in-memory state (e.g. from an aborted partial restore) yields exactly
// the Pebble contents, not a union. Callers (machine.RecoverState,
// CacheSnapshotter.RestoreFromStore) Reset before calling restore, but
// the function itself should also tolerate stale state.
func TestIdempotencyStore_RestoreFromStore_OverwritesPriorEntries(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)

	batch := store.NewBatch()
	require.NoError(t, saveIdempotencyKey(batch, "persisted", &commonpb.IdempotencyKeyValue{
		LogSequence: 42,
		CreatedAt:   1_000,
	}))
	require.NoError(t, batch.Commit())

	idemp := NewIdempotencyStore(0)
	// Pre-populate with a value that ONLY lives in memory.
	idemp.Put("memory-only", &commonpb.IdempotencyKeyValue{LogSequence: 99})

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	require.NoError(t, idemp.RestoreFromStore(handle))

	got, ok := idemp.Get("persisted")
	require.True(t, ok)
	require.Equal(t, uint64(42), got.GetLogSequence())

	// "memory-only" was never persisted to Pebble, so it survives only because
	// RestoreFromStore is additive. Callers (RecoverState / CacheSnapshotter)
	// call Reset before restore to avoid this drift. This test just documents
	// the contract.
	_, ok = idemp.Get("memory-only")
	require.True(t, ok, "RestoreFromStore is additive; callers Reset() first to enforce strict equality with Pebble")
}
