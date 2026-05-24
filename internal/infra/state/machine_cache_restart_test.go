package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
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
	_, err := machine.ApplyEntries(ctx,
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
		proposal.Preload.LastPersistedIndex = machine.Registry.Cache.BaseIndex.Gen0

		_, err := machine.ApplyEntries(ctx,
			makeEntry(t, index, proposal),
		)
		require.NoError(t, err)
	}

	// At this point we've applied entries 1..11.
	// With threshold=5: rotations at index 6 and 11.
	// gen0 has entries from index 10+, gen1 has entries from index 5-9.
	// Entries from index 1-4 have been purged from cache (but still in 0xF1).

	// Verify all volumes are accessible via cache before restart.
	t.Log("Before restart: checking all volumes are in cache")
	verifyAllVolumesInCache(t, machine, dataStore, attrs)

	// ---------------------------------------------------------------
	// Simulate restart: reset cache and restore from 0xFF
	// ---------------------------------------------------------------
	t.Log("Simulating restart: reset cache + restore from 0xFF")
	machine.Registry.Cache.Reset()

	err = machine.cacheSnapshotter.RestoreFromStore()
	require.NoError(t, err)

	// Verify cache is coherent after restore.
	t.Log("After restore: checking all volumes are in cache")
	verifyAllVolumesInCache(t, machine, dataStore, attrs)

	// ---------------------------------------------------------------
	// Entries 12..20: more transactions (triggers more rotations)
	// Rotations at index 16, 21. The rotation at 16 will purge
	// old gen1 data from before the restart.
	// ---------------------------------------------------------------
	for i := 0; i < 9; i++ {
		index := uint64(12 + i)
		proposalID := uint64(200 + i)
		account := accounts[i%len(accounts)]

		proposal := makeProposal(proposalID,
			createTransactionOrder(ledgerName, true,
				newPosting("world", account, "USD", 500),
			),
		)
		proposal.Preload.LastPersistedIndex = machine.Registry.Cache.BaseIndex.Gen0

		_, err := machine.ApplyEntries(ctx,
			makeEntry(t, index, proposal),
		)
		require.NoError(t, err)
	}

	// ---------------------------------------------------------------
	// Final verification: ALL volumes in 0xF1 must be reachable from cache
	// ---------------------------------------------------------------
	t.Log("After restart + more entries: checking all volumes are in cache")
	verifyAllVolumesInCache(t, machine, dataStore, attrs)
}

// verifyAllVolumesInCache checks that every volume key present in the 0xF1
// attribute zone is also present in the in-memory cache (gen0 or gen1).
// This is the critical invariant: the cache must be a superset of what
// CacheGuaranteed could reference.
func verifyAllVolumesInCache(t *testing.T, machine *Machine, store *dal.Store, attrs *attributes.Attributes) {
	t.Helper()

	hasher := attributes.NewKeyHasher(attributes.DefaultSeeds)

	// Iterate all volume entries in 0xF1
	iter, err := attrs.Volume.NewStreamingIter(store, nil)
	require.NoError(t, err)

	var totalAttr, missingFromCache int

	for iter.Next() {
		entry := iter.Entry()
		totalAttr++

		// Compute the U128 from the canonical key
		u128, _ := hasher.MakeKey(entry.CanonicalKey)

		// Check if the key is in gen0 or gen1
		_, inCache := machine.Registry.Cache.Volumes.Get(u128)
		if !inCache {
			var vk domain.VolumeKey
			if unmarshalErr := vk.Unmarshal(entry.CanonicalKey); unmarshalErr == nil {
				t.Errorf("Volume %d/%s/%s (key=%x) is in 0xF1 but NOT in cache",
					vk.LedgerID, vk.Account, vk.Asset, entry.CanonicalKey)
			} else {
				t.Errorf("Volume (key=%x) is in 0xF1 but NOT in cache", entry.CanonicalKey)
			}

			missingFromCache++
		}
	}

	require.NoError(t, iter.Close())
	require.NoError(t, iter.Err())

	t.Logf("0xF1 has %d volumes, %d missing from cache", totalAttr, missingFromCache)

	if missingFromCache > 0 {
		t.Logf("Cache state: gen=%d, gen0Base=%d, gen1Base=%d, gen0Size=%d, gen1Size=%d",
			machine.Registry.Cache.CurrentGeneration(),
			machine.Registry.Cache.BaseIndex.Gen0,
			machine.Registry.Cache.BaseIndex.Gen1,
			machine.Registry.Cache.Volumes.Gen0().Size(),
			machine.Registry.Cache.Volumes.Gen1().Size(),
		)
		t.Fatalf("%d volumes in 0xF1 are missing from cache — CacheGuaranteed proposals would fail", missingFromCache)
	}
}
