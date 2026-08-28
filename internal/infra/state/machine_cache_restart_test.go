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
// The restart scenario:
//  1. Process entries 1..N (some entries modify volumes, cache rotations happen)
//  2. Simulate restart: reset cache → restore from Pebble 0xFF
//  3. Continue processing entries N+1..M (more rotations happen)
//  4. Verify the active persisted 0xFF volume mirror and the corresponding
//     in-memory cache generations contain exactly the same keys
//
// Historical volumes can legitimately remain in the 0xF1 attribute zone after
// they leave the two active cache generations, so 0xF1 is not part of the
// equality asserted here.
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
	// Final verification: the cache and active 0xFF mirror must still match
	// bidirectionally after further entries and rotations.
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

// verifyCacheMatchesPebbleFF checks that the active in-memory volume cache and
// its persisted 0xFF mirror contain exactly the same keys in each generation.
func verifyCacheMatchesPebbleFF(t *testing.T, machine *Machine, store *dal.Store) {
	t.Helper()

	reader, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() {
		require.NoError(t, reader.Close())
	}()

	currentGen := machine.Registry.Cache.CurrentGeneration()
	gen0Byte := byte(currentGen % 2)
	gen1Byte := byte((currentGen + 1) % 2)

	var missingFromPebble int

	for id := range machine.Registry.Cache.Volumes.Gen0().Iter() {
		if !hasCacheZoneEntry(t, reader, gen0Byte, dal.SubAttrVolume, id) {
			t.Errorf("Volume U128=%x in memory gen0 but NOT in 0xFF byte %d", id, gen0Byte)
			missingFromPebble++
		}
	}

	for id := range machine.Registry.Cache.Volumes.Gen1().Iter() {
		if !hasCacheZoneEntry(t, reader, gen1Byte, dal.SubAttrVolume, id) {
			t.Errorf("Volume U128=%x in memory gen1 but NOT in 0xFF byte %d", id, gen1Byte)
			missingFromPebble++
		}
	}

	require.Zero(t, missingFromPebble, "in-memory cache has entries missing from the active 0xFF mirror")

	missingFromMemory := countPebbleFFVolumesMissingFromMemory(t, reader, gen0Byte, "gen0", func(id attributes.U128) bool {
		_, ok := machine.Registry.Cache.Volumes.Gen0().Get(id)

		return ok
	})
	missingFromMemory += countPebbleFFVolumesMissingFromMemory(t, reader, gen1Byte, "gen1", func(id attributes.U128) bool {
		_, ok := machine.Registry.Cache.Volumes.Gen1().Get(id)

		return ok
	})

	require.Zero(t, missingFromMemory, "active 0xFF mirror has entries missing from the in-memory cache")
}

func countPebbleFFVolumesMissingFromMemory(
	t *testing.T,
	reader dal.PebbleReader,
	genByte byte,
	generation string,
	contains func(attributes.U128) bool,
) int {
	t.Helper()

	lower := []byte{dal.ZoneCache, genByte, dal.SubAttrVolume}
	upper := []byte{dal.ZoneCache, genByte, dal.SubAttrVolume + 1}

	iter, err := dal.NewBoundedIter(reader, lower, upper)
	require.NoError(t, err)

	missing := 0

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		if len(key) != 3+16 {
			t.Errorf("Volume row in 0xFF byte %d has key length %d, expected %d", genByte, len(key), 3+16)

			continue
		}

		id := attributes.U128FromBytes(key[3:])
		if !contains(id) {
			t.Errorf("Volume U128=%x in 0xFF byte %d but NOT in memory %s", id, genByte, generation)
			missing++
		}
	}

	iterErr := iter.Error()
	closeErr := iter.Close()
	require.NoError(t, iterErr)
	require.NoError(t, closeErr)

	return missing
}

func hasCacheZoneEntry(t *testing.T, store dal.PebbleGetter, genByte, cacheType byte, id attributes.U128) bool {
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

	require.NoError(t, closer.Close())

	return true
}
