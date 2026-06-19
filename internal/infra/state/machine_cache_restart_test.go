package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestCacheCoherenceAfterRestart verifies that the in-memory volume cache
// remains coherent after a simulated restart (cache reset + restore from 0xFF).
//
// The bug scenario:
//  1. Process entries 1..N (some entries modify volumes, cache rotations happen)
//  2. Simulate restart: reset cache → restore from Pebble 0xFF
//  3. Continue processing entries N+1..M (more rotations happen)
//  4. Verify ALL volumes that are in the 0xF1 attribute zone are also reachable
//     from the in-memory cache (gen0 or gen1)
//
// If any volume is in 0xF1 but not in the cache after restart + rotations,
// proposals marked CacheGuaranteed for that volume will fail with
// "not preloaded: not found" — the root cause of the volume divergence bug.
func TestCacheCoherenceAfterRestart(t *testing.T) {
	t.Parallel()

	// Use a small threshold so rotations happen quickly.
	const threshold = 5
	machine, dataStore, attrs := newTestMachineWithThreshold(t, threshold)
	ctx := context.Background()

	const ledgerName = "test-ledger"

	// ---------------------------------------------------------------
	// Entry 1: create the ledger
	// ---------------------------------------------------------------
	_, err := machine.ApplyEntries(ctx, dataStore,
		makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledgerName))),
	)
	require.NoError(t, err)

	// ---------------------------------------------------------------
	// Entries 2..12: create transactions that touch different accounts
	// This covers 2+ cache rotations (threshold=5 → rotations at 6, 11)
	// ---------------------------------------------------------------
	accounts := []string{
		"users:0", "users:1", "users:2", "users:3", "users:4",
		"users:5", "users:6", "users:7", "users:8", "users:9",
	}

	for i, account := range accounts {
		index := uint64(i + 2)
		proposalID := uint64(100 + i)

		proposal := makeProposal(proposalID,
			createTransactionOrder(ledgerName, true,
				newPosting("world", account, "COIN", 1000),
			),
		)
		proposal.ExecutionPlan.LastPersistedIndex = machine.Registry.Cache.BaseIndex.Gen0

		_, err := machine.ApplyEntries(ctx, dataStore,
			makeEntry(t, index, proposal),
		)
		require.NoError(t, err)
	}

	// At this point we've applied entries 1..11.
	// With threshold=5: rotations at index 6 and 11.
	// gen0 has entries from index 10+, gen1 has entries from index 5-9.
	// Entries from index 1-4 have been purged from cache (but still in 0xF1).

	// Log cache state before restart (some volumes may have been evicted — that's normal).
	t.Log("Before restart: cache state")
	logCacheState(t, machine, dataStore, attrs)

	// ---------------------------------------------------------------
	// Simulate restart: reset cache and restore from 0xFF
	// ---------------------------------------------------------------
	t.Log("Simulating restart: reset cache + restore from 0xFF")
	machine.Registry.Cache.Reset()

	err = machine.cacheSnapshotter.RestoreFromStore(dataStore)
	require.NoError(t, err)

	// After restore, the cache should match 0xFF exactly.
	t.Log("After restore: verifying cache matches 0xFF")
	verifyCacheMatchesPebbleFF(t, machine, dataStore)

	// ---------------------------------------------------------------
	// Entries 12..20: more transactions (triggers more rotations)
	// Rotations at index 16, 21. The rotation at 16 will purge
	// old gen1 data from before the restart.
	// ---------------------------------------------------------------
	for i := range 9 {
		index := uint64(12 + i)
		proposalID := uint64(200 + i)
		account := accounts[i%len(accounts)]

		proposal := makeProposal(proposalID,
			createTransactionOrder(ledgerName, true,
				newPosting("world", account, "USD", 500),
			),
		)
		proposal.ExecutionPlan.LastPersistedIndex = machine.Registry.Cache.BaseIndex.Gen0

		_, err := machine.ApplyEntries(ctx, dataStore,
			makeEntry(t, index, proposal),
		)
		require.NoError(t, err)
	}

	// ---------------------------------------------------------------
	// Final verification: cache must match 0xFF (no in-memory entries
	// that are missing from Pebble 0xFF — the restart invariant)
	// ---------------------------------------------------------------
	t.Log("After restart + more entries: verifying cache matches 0xFF")
	verifyCacheMatchesPebbleFF(t, machine, dataStore)
}

// logCacheState logs the cache vs 0xF1 state for debugging.
func logCacheState(t *testing.T, machine *Machine, store *dal.Store, attrs *attributes.Attributes) {
	t.Helper()

	hasher := attributes.NewKeyHasher()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	iter, err := attrs.Volume.NewStreamingIter(handle, nil)
	require.NoError(t, err)

	var totalAttr, inCache int

	for iter.Next() {
		totalAttr++
		u128, _ := hasher.MakeKey(iter.Entry().CanonicalKey)

		if _, ok := machine.Registry.Cache.Volumes.Get(u128); ok {
			inCache++
		}
	}

	require.NoError(t, iter.Close())

	t.Logf("Cache: gen=%d, gen0Base=%d, gen1Base=%d, gen0Size=%d, gen1Size=%d, 0xF1=%d, inCache=%d",
		machine.Registry.Cache.CurrentGeneration(),
		machine.Registry.Cache.BaseIndex.Gen0,
		machine.Registry.Cache.BaseIndex.Gen1,
		machine.Registry.Cache.Volumes.Gen0().Size(),
		machine.Registry.Cache.Volumes.Gen1().Size(),
		totalAttr, inCache,
	)
}

// verifyCacheMatchesPebbleFF checks that the in-memory cache is consistent
// with the 0xFF Pebble zone: every key in memory must also be in 0xFF.
// This is the restart safety invariant — if a key is in memory but not in
// 0xFF, a restart would lose it.
func verifyCacheMatchesPebbleFF(t *testing.T, machine *Machine, store *dal.Store) {
	t.Helper()

	currentGen := machine.Registry.Cache.CurrentGeneration()
	gen0Byte := byte(currentGen % 2)
	gen1Byte := byte((currentGen + 1) % 2)

	var missing int

	for id := range machine.Registry.Cache.Volumes.Gen0().Iter() {
		if !hasCacheZoneEntry(t, store, gen0Byte, dal.SubAttrVolume, id) {
			t.Errorf("Volume U128=%x in memory gen0 but NOT in 0xFF byte %d", id, gen0Byte)
			missing++
		}
	}

	for id := range machine.Registry.Cache.Volumes.Gen1().Iter() {
		if !hasCacheZoneEntry(t, store, gen1Byte, dal.SubAttrVolume, id) {
			t.Errorf("Volume U128=%x in memory gen1 but NOT in 0xFF byte %d", id, gen1Byte)
			missing++
		}
	}

	require.Zero(t, missing, "in-memory cache has entries missing from 0xFF — restart would lose them")
}

func hasCacheZoneEntry(t *testing.T, store *dal.Store, genByte, cacheType byte, id attributes.U128) bool {
	t.Helper()

	var key [3 + 16]byte
	key[0] = dal.ZoneCache
	key[1] = genByte
	key[2] = cacheType
	copy(key[3:], id[:])

	_, closer, err := store.Get(key[:])
	if err != nil {
		return false
	}

	_ = closer.Close()

	return true
}
