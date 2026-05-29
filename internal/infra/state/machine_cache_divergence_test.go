package state

import (
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/cache"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestCacheDivergenceAfterRestart reproduces the volume corruption bug by
// simulating a leader-follower pair where:
//  1. Both machines apply the same entries with preloads computed by the leader
//  2. The follower crashes and restarts (cache reset + RestoreFromStore)
//  3. Post-restart entries use the leader's cache state (CacheGuaranteed)
//     which may not match the follower's restored cache
//
// The test checks that volume values stay consistent between leader and follower.
func TestCacheDivergenceAfterRestart(t *testing.T) {
	t.Parallel()

	const threshold = 3 // very small to trigger many rotations
	const ledgerName = "test-ledger"

	leader, leaderStore, _ := newTestMachineWithThreshold(t, threshold)
	follower, followerStore, _ := newTestMachineWithThreshold(t, threshold)
	ctx := context.Background()

	// Track the next entry index
	nextIndex := uint64(1)

	// Helper: compute preloads using LEADER's cache, then apply on both machines
	applyOnBoth := func(t *testing.T, orders ...*raftcmdpb.Order) {
		t.Helper()

		proposal := buildProposalWithLeaderPreloads(t, leader, nextIndex, threshold, orders...)
		entry := mustMakeEntry(t, nextIndex, proposal)

		resultL, err := leader.ApplyEntries(ctx, entry)
		require.NoError(t, err, "leader apply entry %d", nextIndex)

		resultF, err := follower.ApplyEntries(ctx, entry)
		require.NoError(t, err, "follower apply entry %d", nextIndex)

		// Both should produce the same result (accept or reject)
		leaderErr := resultErrorString(resultL)
		followerErr := resultErrorString(resultF)

		if leaderErr != followerErr {
			t.Fatalf("DIVERGENCE at entry %d: leader=%q, follower=%q",
				nextIndex, leaderErr, followerErr)
		}

		nextIndex++
	}

	// Step 1: Create ledger on both
	applyOnBoth(t, createLedgerOrder(ledgerName))

	// Step 2: Run several transactions to populate cache and trigger rotations
	accounts := []string{"users:a", "users:b", "users:c", "users:d", "users:e"}
	for i := range 20 {
		account := accounts[i%len(accounts)]
		applyOnBoth(t,
			createTransactionOrder(ledgerName, true,
				newPosting("world", account, "COIN", 100),
			),
		)
	}

	t.Logf("Before crash: nextIndex=%d, leader gen=%d, follower gen=%d",
		nextIndex,
		leader.Registry.Cache.CurrentGeneration(),
		follower.Registry.Cache.CurrentGeneration(),
	)

	// Step 3: Crash the follower - reset cache and restore from 0xFF
	t.Log("=== CRASHING FOLLOWER ===")
	follower.Registry.Cache.Reset()
	err := follower.cacheSnapshotter.RestoreFromStore()
	require.NoError(t, err)

	t.Logf("After restore: follower gen=%d, gen0Size=%d, gen1Size=%d",
		follower.Registry.Cache.CurrentGeneration(),
		follower.Registry.Cache.Volumes.Gen0().Size(),
		follower.Registry.Cache.Volumes.Gen1().Size(),
	)

	// Step 4: Continue applying entries post-restart
	// The leader computes preloads from its live cache (which has all keys).
	// The follower uses the restored cache (which might be missing keys).
	for i := range 30 {
		account := accounts[i%len(accounts)]
		applyOnBoth(t,
			createTransactionOrder(ledgerName, true,
				newPosting("world", account, "COIN", 50),
			),
		)
	}

	// Step 5: Verify volume consistency between leader and follower
	t.Log("=== VERIFYING VOLUME CONSISTENCY ===")
	verifyVolumeConsistency(t, leader, leaderStore, follower, followerStore)
}

// TestCacheDivergenceWithPipeliningBatches simulates the pipelining scenario
// where a batch commits on the follower but the next batch (with touches) doesn't.
func TestCacheDivergenceWithPipeliningBatches(t *testing.T) {
	t.Parallel()

	const threshold = 3
	const ledgerName = "test-ledger"

	leader, leaderStore, _ := newTestMachineWithThreshold(t, threshold)
	follower, followerStore, _ := newTestMachineWithThreshold(t, threshold)
	ctx := context.Background()

	nextIndex := uint64(1)

	applyOnBoth := func(t *testing.T, orders ...*raftcmdpb.Order) {
		t.Helper()

		proposal := buildProposalWithLeaderPreloads(t, leader, nextIndex, threshold, orders...)
		entry := mustMakeEntry(t, nextIndex, proposal)

		resultL, err := leader.ApplyEntries(ctx, entry)
		require.NoError(t, err)

		resultF, err := follower.ApplyEntries(ctx, entry)
		require.NoError(t, err)

		leaderErr := resultErrorString(resultL)
		followerErr := resultErrorString(resultF)
		if leaderErr != followerErr {
			t.Fatalf("DIVERGENCE at entry %d: leader=%q, follower=%q",
				nextIndex, leaderErr, followerErr)
		}

		nextIndex++
	}

	applyOnBoth(t, createLedgerOrder(ledgerName))

	// Build up cache with many accounts across multiple rotations
	accounts := make([]string, 20)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("users:%03d", i)
	}

	for i := range 40 {
		applyOnBoth(t,
			createTransactionOrder(ledgerName, true,
				newPosting("world", accounts[i%len(accounts)], "COIN", 100),
			),
		)
	}

	// Simulate crash right at a rotation boundary
	// Find the next rotation point
	rotationIndex := nextIndex
	for {
		g := cache.Gen(rotationIndex, threshold)
		if g != leader.Registry.Cache.CurrentGeneration() {
			break
		}
		rotationIndex++
	}

	// Apply entries up to just BEFORE the rotation
	for nextIndex < rotationIndex {
		applyOnBoth(t,
			createTransactionOrder(ledgerName, true,
				newPosting("world", accounts[int(nextIndex)%len(accounts)], "COIN", 10),
			),
		)
	}

	t.Logf("At rotation boundary: nextIndex=%d, gen=%d",
		nextIndex, leader.Registry.Cache.CurrentGeneration())

	// Crash follower right before the rotation
	t.Log("=== CRASHING FOLLOWER AT ROTATION BOUNDARY ===")
	follower.Registry.Cache.Reset()
	err := follower.cacheSnapshotter.RestoreFromStore()
	require.NoError(t, err)

	// Apply more entries that cross the rotation boundary
	for i := range 30 {
		applyOnBoth(t,
			createTransactionOrder(ledgerName, true,
				newPosting("world", accounts[i%len(accounts)], "COIN", 25),
			),
		)
	}

	verifyVolumeConsistency(t, leader, leaderStore, follower, followerStore)
}

// TestCacheDivergenceBatchReplay simulates the spool replay scenario where
// the follower applies a large batch of entries after restart.
func TestCacheDivergenceBatchReplay(t *testing.T) {
	t.Parallel()

	const threshold = 3
	const ledgerName = "test-ledger"

	leader, leaderStore, _ := newTestMachineWithThreshold(t, threshold)
	follower, followerStore, _ := newTestMachineWithThreshold(t, threshold)
	ctx := context.Background()

	nextIndex := uint64(1)

	applyOnBoth := func(t *testing.T, orders ...*raftcmdpb.Order) {
		t.Helper()

		proposal := buildProposalWithLeaderPreloads(t, leader, nextIndex, threshold, orders...)
		entry := mustMakeEntry(t, nextIndex, proposal)

		resultL, err := leader.ApplyEntries(ctx, entry)
		require.NoError(t, err)
		resultF, err := follower.ApplyEntries(ctx, entry)
		require.NoError(t, err)

		leaderErr := resultErrorString(resultL)
		followerErr := resultErrorString(resultF)
		if leaderErr != followerErr {
			t.Fatalf("DIVERGENCE at entry %d: leader=%q, follower=%q",
				nextIndex, leaderErr, followerErr)
		}

		nextIndex++
	}

	applyOnBoth(t, createLedgerOrder(ledgerName))

	accounts := make([]string, 10)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("users:%03d", i)
	}

	// Apply 15 entries on both (several rotations with threshold=3)
	for i := range 15 {
		applyOnBoth(t,
			createTransactionOrder(ledgerName, true,
				newPosting("world", accounts[i%len(accounts)], "COIN", 100),
			),
		)
	}

	crashIndex := nextIndex
	t.Logf("Crashing follower at index %d, gen=%d", crashIndex, follower.Registry.Cache.CurrentGeneration())

	// Now: leader continues alone for many entries, follower is "dead"
	leaderEntries := make([]raftpb.Entry, 0, 30)
	for i := range 30 {
		account := accounts[i%len(accounts)]
		proposal := buildProposalWithLeaderPreloads(t, leader, nextIndex, threshold,
			createTransactionOrder(ledgerName, true,
				newPosting("world", account, "COIN", 50),
			),
		)
		entry := mustMakeEntry(t, nextIndex, proposal)

		// Apply on leader only
		_, err := leader.ApplyEntries(ctx, entry)
		require.NoError(t, err, "leader apply entry %d", nextIndex)

		leaderEntries = append(leaderEntries, entry)
		nextIndex++
	}

	t.Logf("Leader advanced to index %d, gen=%d",
		nextIndex-1, leader.Registry.Cache.CurrentGeneration())

	// Crash and restore follower
	t.Log("=== CRASHING AND RESTORING FOLLOWER ===")
	follower.Registry.Cache.Reset()
	err := follower.cacheSnapshotter.RestoreFromStore()
	require.NoError(t, err)

	t.Logf("Follower restored: gen=%d, gen0=%d, gen1=%d",
		follower.Registry.Cache.CurrentGeneration(),
		follower.Registry.Cache.Volumes.Gen0().Size(),
		follower.Registry.Cache.Volumes.Gen1().Size(),
	)

	// Replay all leader entries on follower (simulates spool catchup)
	// Apply in batches to simulate realistic behavior
	batchSize := 10
	for i := 0; i < len(leaderEntries); i += batchSize {
		end := min(i+batchSize, len(leaderEntries))
		batch := leaderEntries[i:end]

		result, err := follower.ApplyEntries(ctx, batch...)
		require.NoError(t, err, "follower replay batch starting at %d", batch[0].Index)

		// Check each result for divergence
		for j, r := range result.Results {
			if r.Error != nil {
				t.Logf("Follower REJECTED entry %d: %v", batch[j].Index, r.Error)
			}
		}
	}

	// Verify consistency
	t.Log("=== VERIFYING VOLUME CONSISTENCY AFTER BATCH REPLAY ===")
	verifyVolumeConsistency(t, leader, leaderStore, follower, followerStore)
}

// TestCacheDivergencePipelinedAdmission simulates the REAL bug scenario:
// the leader admits MULTIPLE proposals from the SAME cache state before any
// of them are applied. This creates entries with stale CacheGuaranteed that
// don't include touches for keys that will move to gen1 after rotation.
//
// With threshold=3 and a pipeline depth of 6+ entries:
//   - Entries are admitted when K is in gen0 → CacheGuaranteed
//   - First entry triggers rotation → K moves to gen1
//   - Next entries don't have Touch for K (CacheGuaranteed in preloads)
//   - Second rotation → K in gen1 is PURGED
//   - K is permanently lost from the follower's cache
func TestCacheDivergencePipelinedAdmission(t *testing.T) {
	t.Parallel()

	const threshold = 3
	const ledgerName = "test-ledger"

	leader, leaderStore, _ := newTestMachineWithThreshold(t, threshold)
	follower, followerStore, _ := newTestMachineWithThreshold(t, threshold)
	ctx := context.Background()

	nextIndex := uint64(1)

	// Helper: compute + apply synchronously on both
	applyOnBoth := func(t *testing.T, orders ...*raftcmdpb.Order) {
		t.Helper()

		proposal := buildProposalWithLeaderPreloads(t, leader, nextIndex, threshold, orders...)
		entry := mustMakeEntry(t, nextIndex, proposal)

		_, err := leader.ApplyEntries(ctx, entry)
		require.NoError(t, err)
		_, err = follower.ApplyEntries(ctx, entry)
		require.NoError(t, err)

		nextIndex++
	}

	// Step 1: Create ledger
	applyOnBoth(t, createLedgerOrder(ledgerName))

	// Step 2: Build up state with several rotations
	for i := range 12 {
		applyOnBoth(t,
			createTransactionOrder(ledgerName, true,
				newPosting("world", fmt.Sprintf("users:%d", i), "COIN", 100),
			),
		)
	}

	t.Logf("Setup complete: nextIndex=%d, gen=%d", nextIndex, leader.Registry.Cache.CurrentGeneration())

	// Step 3: Crash the follower
	t.Log("=== CRASHING FOLLOWER ===")
	follower.Registry.Cache.Reset()
	err := follower.cacheSnapshotter.RestoreFromStore()
	require.NoError(t, err)

	t.Logf("Follower restored: gen=%d", follower.Registry.Cache.CurrentGeneration())

	// Step 4: Simulate PIPELINED admission on the leader.
	// The admission goroutine and FSM goroutine run concurrently. The admission
	// reads the cache (CheckCache) while the FSM modifies it (rotation, touch).
	// We simulate this by alternating: admit some entries, apply some, admit more.
	for batch := range 5 {
		// Phase A: Leader admits several proposals from CURRENT cache state
		// (before any are applied - simulates admission pipeline depth)
		pipelineDepth := threshold + 1 // cross 1 rotation boundary

		admittedEntries := make([]raftpb.Entry, 0, pipelineDepth)
		for i := range uint64(pipelineDepth) {
			idx := nextIndex + i
			account := fmt.Sprintf("users:%d", int(idx)%5)
			proposal := buildProposalWithLeaderPreloads(t, leader, idx, threshold,
				createTransactionOrder(ledgerName, true,
					newPosting("world", account, "COIN", 10),
				),
			)
			admittedEntries = append(admittedEntries, mustMakeEntry(t, idx, proposal))
		}

		// Phase B: Apply all admitted entries on leader
		var leaderRejects int

		for _, entry := range admittedEntries {
			result, err := leader.ApplyEntries(ctx, entry)
			require.NoError(t, err, "leader apply entry %d", entry.Index)

			for _, r := range result.Results {
				if r.Error != nil {
					leaderRejects++
					t.Logf("Leader REJECTED entry %d: %v", entry.Index, r.Error)
				}
			}
		}

		// Phase C: Apply all entries on follower (catchup batch)
		result, err := follower.ApplyEntries(ctx, admittedEntries...)
		require.NoError(t, err, "follower apply batch %d", batch)

		var followerRejects int

		for i, r := range result.Results {
			if r.Error != nil {
				followerRejects++
				t.Logf("Follower REJECTED entry %d: %v",
					admittedEntries[i].Index, r.Error)
			}
		}

		if leaderRejects != followerRejects {
			t.Logf("WARNING batch %d: leader rejected %d, follower rejected %d",
				batch, leaderRejects, followerRejects)
		}

		nextIndex += uint64(pipelineDepth)
	}

	// Step 5: Verify volume consistency
	t.Log("=== VERIFYING VOLUME CONSISTENCY ===")
	verifyVolumeConsistency(t, leader, leaderStore, follower, followerStore)
}

// TestCacheDivergenceStress runs many iterations with random crash points
// to maximize chances of hitting the cache divergence.
func TestCacheDivergenceStress(t *testing.T) {
	t.Parallel()

	for threshold := uint64(2); threshold <= 5; threshold++ {
		for crashAt := 3; crashAt <= 20; crashAt++ {
			for pipelineDepth := 1; pipelineDepth <= 8; pipelineDepth++ {
				name := fmt.Sprintf("threshold=%d/crash=%d/pipeline=%d", threshold, crashAt, pipelineDepth)
				threshold := threshold
				crashAt := crashAt

				t.Run(name, func(t *testing.T) {
					t.Parallel()
					runCacheDivergenceScenario(t, threshold, crashAt, pipelineDepth)
				})
			}
		}
	}
}

func runCacheDivergenceScenario(t *testing.T, threshold uint64, crashAfterN, pipelineDepth int) {
	t.Helper()

	const ledgerName = "test-ledger"

	leader, leaderStore, _ := newTestMachineWithThreshold(t, threshold)
	follower, followerStore, _ := newTestMachineWithThreshold(t, threshold)
	ctx := context.Background()

	nextIndex := uint64(1)

	// Synchronous apply on both
	applyOnBoth := func(orders ...*raftcmdpb.Order) {
		proposal := buildProposalWithLeaderPreloads(t, leader, nextIndex, threshold, orders...)
		entry := mustMakeEntry(t, nextIndex, proposal)
		_, err := leader.ApplyEntries(ctx, entry)
		require.NoError(t, err)
		_, err = follower.ApplyEntries(ctx, entry)
		require.NoError(t, err)
		nextIndex++
	}

	// Create ledger
	applyOnBoth(createLedgerOrder(ledgerName))

	// Build up state
	for i := range crashAfterN {
		applyOnBoth(createTransactionOrder(ledgerName, true,
			newPosting("world", fmt.Sprintf("u:%d", i%3), "COIN", 100),
		))
	}

	// Crash follower
	follower.Registry.Cache.Reset()
	err := follower.cacheSnapshotter.RestoreFromStore()
	require.NoError(t, err)

	// Leader continues with pipelined admission
	for range 3 {
		// Admit pipelineDepth entries from SAME leader cache state
		entries := make([]raftpb.Entry, 0, pipelineDepth)
		for i := range pipelineDepth {
			idx := nextIndex + uint64(i)
			proposal := buildProposalWithLeaderPreloads(t, leader, idx, threshold,
				createTransactionOrder(ledgerName, true,
					newPosting("world", fmt.Sprintf("u:%d", int(idx)%3), "COIN", 10),
				),
			)
			entries = append(entries, mustMakeEntry(t, idx, proposal))
		}

		// Apply on leader one by one
		for _, entry := range entries {
			_, err := leader.ApplyEntries(ctx, entry)
			require.NoError(t, err)
		}

		// Apply on follower as a batch (catchup)
		result, err := follower.ApplyEntries(ctx, entries...)
		require.NoError(t, err)

		// Check for divergence in rejections
		for i, r := range result.Results {
			if r.Error != nil && r.AppliedIndex > 0 {
				// Entry was applied on leader but might not match follower
				t.Logf("Follower rejected entry %d: %v", entries[i].Index, r.Error)
			}
		}

		nextIndex += uint64(pipelineDepth)
	}

	// Final consistency check
	leaderVols := readAllVolumes(t, leaderStore)
	followerVols := readAllVolumes(t, followerStore)

	for key, lv := range leaderVols {
		fv, ok := followerVols[key]
		if !ok || lv != fv {
			t.Fatalf("DIVERGENCE: key=%s leader=%s follower=%s (ok=%v)", key, lv, fv, ok)
		}
	}
}

// buildProposalWithLeaderPreloads builds a proposal using the LEADER's cache
// to determine which preloads to include (simulating the admission layer).
func buildProposalWithLeaderPreloads(
	t *testing.T,
	leader *Machine,
	nextIndex uint64,
	threshold uint64,
	orders ...*raftcmdpb.Order,
) *raftcmdpb.Proposal {
	t.Helper()

	boundary := cache.BoundaryIndex(nextIndex, threshold)

	var preloads []*raftcmdpb.Preload
	var touches []*raftcmdpb.CacheTouch

	// For CreateLedger, we need to preload the ledger key (it won't be in cache)
	for _, order := range orders {
		if cl := order.GetCreateLedger(); cl != nil {
			// CreateLedger: the ledger doesn't exist yet, no preload needed
			continue
		}

		apply := order.GetApply()
		if apply == nil {
			continue
		}

		ledger := apply.GetLedger()

		// Check LedgerInfo in leader's cache
		ledgerKey := domain.LedgerKey{Name: ledger}
		ledgerCanonical := ledgerKey.Bytes()
		ledgerU128, ledgerTag := attributes.MakeKey(attributes.DefaultSeeds, ledgerCanonical)

		switch leader.Registry.Cache.Ledgers.CheckCache(nextIndex, ledgerU128) {
		case cache.CacheGuaranteed:
			// Nothing - leader guarantees it's in cache
		case cache.CacheNeedsTouch:
			touches = append(touches, &raftcmdpb.CacheTouch{
				Id:       ledgerU128[:],
				AttrType: uint32(dal.SubAttrLedger),
			})
		case cache.CacheMiss:
			// Load from leader's Pebble
			info, _, err := leader.Registry.Ledgers.Get(ledgerCanonical)
			if err == nil && info != nil {
				preloads = append(preloads, buildLedgerPreloadForTest(ledgerU128, ledgerTag, info))
			}
		}

		// Check Boundaries in leader's cache
		switch leader.Registry.Cache.Boundaries.CheckCache(nextIndex, ledgerU128) {
		case cache.CacheGuaranteed:
		case cache.CacheNeedsTouch:
			touches = append(touches, &raftcmdpb.CacheTouch{
				Id:       ledgerU128[:],
				AttrType: uint32(dal.SubAttrBoundary),
			})
		case cache.CacheMiss:
			boundaries, _, err := leader.Registry.Boundaries.Get(ledgerCanonical)
			if err == nil && boundaries != nil {
				preloads = append(preloads, buildBoundaryPreloadForTest(ledgerU128, ledgerTag, boundaries))
			}
		}

		// Check volumes for each posting
		if ct := apply.GetCreateTransaction(); ct != nil {
			ledgerInfo, _ := leader.writeSet.GetLedger(ledger)
			if ledgerInfo == nil {
				// Try from parent store
				ledgerInfo, _, _ = leader.Registry.Ledgers.GetKey(ledgerKey)
			}

			ledgerID := uint32(1)
			if ledgerInfo != nil {
				ledgerID = ledgerInfo.GetId()
			}

			for _, p := range ct.GetPostings() {
				for _, account := range []string{p.GetSource(), p.GetDestination()} {
					volKey := domain.VolumeKey{
						AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: account},
						Asset:      p.GetAsset(),
					}
					volCanonical := volKey.Bytes()
					volU128, volTag := attributes.MakeKey(attributes.DefaultSeeds, volCanonical)

					switch leader.Registry.Cache.Volumes.CheckCache(nextIndex, volU128) {
					case cache.CacheGuaranteed:
						// Nothing
					case cache.CacheNeedsTouch:
						touches = append(touches, &raftcmdpb.CacheTouch{
							Id:       volU128[:],
							AttrType: uint32(dal.SubAttrVolume),
						})
					case cache.CacheMiss:
						vol, _, err := leader.Registry.Volumes.Get(volCanonical)
						if err != nil || vol == nil {
							vol = &raftcmdpb.VolumePair{
								Input:  commonpb.NewUint256FromUint64(0),
								Output: commonpb.NewUint256FromUint64(0),
							}
						}
						preloads = append(preloads, &raftcmdpb.Preload{
							Type: &raftcmdpb.Preload_Volume{
								Volume: &raftcmdpb.PreloadVolume{
									Id:    &raftcmdpb.AttributeID{Id: volU128[:], Tag: volTag},
									Value: vol,
								},
							},
						})
					}
				}
			}
		}
	}

	return &raftcmdpb.Proposal{
		Id:             nextIndex,
		PredictedIndex: nextIndex,
		Orders:         orders,
		Date:           &commonpb.Timestamp{Data: 1700000000 + nextIndex},
		Preload: &raftcmdpb.PreloadSet{
			Preloads:           preloads,
			Touches:            touches,
			LastPersistedIndex: boundary,
		},
	}
}

func buildLedgerPreloadForTest(u128 attributes.U128, tag uint64, info *commonpb.LedgerInfo) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_Ledger{
			Ledger: &raftcmdpb.PreloadLedger{
				Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: tag},
				Value: info,
			},
		},
	}
}

func buildBoundaryPreloadForTest(u128 attributes.U128, tag uint64, boundaries *raftcmdpb.LedgerBoundaries) *raftcmdpb.Preload {
	return &raftcmdpb.Preload{
		Type: &raftcmdpb.Preload_Boundary{
			Boundary: &raftcmdpb.PreloadBoundary{
				Id:    &raftcmdpb.AttributeID{Id: u128[:], Tag: tag},
				Value: boundaries,
			},
		},
	}
}

func mustMakeEntry(t *testing.T, index uint64, proposal *raftcmdpb.Proposal) raftpb.Entry {
	t.Helper()

	data, err := proto.Marshal(proposal)
	require.NoError(t, err)

	return raftpb.Entry{
		Index: index,
		Term:  1,
		Type:  raftpb.EntryNormal,
		Data:  data,
	}
}

func resultErrorString(result *ApplyEntriesResult) string {
	if result == nil || len(result.Results) == 0 {
		return ""
	}

	for _, r := range result.Results {
		if r.Error != nil {
			return r.Error.Error()
		}
	}

	return ""
}

// verifyVolumeConsistency checks that all volumes in the leader's Pebble store
// match the follower's Pebble store.
func verifyVolumeConsistency(t *testing.T, leader *Machine, leaderStore *dal.Store, follower *Machine, followerStore *dal.Store) {
	t.Helper()

	leaderVolumes := readAllVolumes(t, leaderStore)
	followerVolumes := readAllVolumes(t, followerStore)

	require.Equal(t, len(leaderVolumes), len(followerVolumes),
		"volume count mismatch: leader=%d, follower=%d", len(leaderVolumes), len(followerVolumes))

	var mismatches int

	for key, leaderVol := range leaderVolumes {
		followerVol, ok := followerVolumes[key]
		if !ok {
			t.Errorf("Volume %x exists on leader but not on follower", key)
			mismatches++

			continue
		}

		if leaderVol != followerVol {
			t.Errorf("Volume %x mismatch: leader=%s, follower=%s", key, leaderVol, followerVol)
			mismatches++
		}
	}

	if mismatches > 0 {
		t.Fatalf("%d volume mismatches between leader and follower", mismatches)
	}

	t.Logf("Volume consistency verified: %d volumes match", len(leaderVolumes))
}

func readAllVolumes(t *testing.T, store *dal.Store) map[string]string {
	t.Helper()

	lower := []byte{dal.ZoneAttributes, dal.SubAttrVolume}
	upper := []byte{dal.ZoneAttributes, dal.SubAttrVolume + 1}

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	iter, err := handle.NewIter(nil)
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	volumes := make(map[string]string)

	iter.SeekGE(lower)
	for iter.Valid() {
		key := iter.Key()
		if len(key) == 0 || key[0] > upper[0] || (key[0] == upper[0] && key[1] >= upper[1]) {
			break
		}

		val, err := iter.ValueAndErr()
		require.NoError(t, err)

		volumes[hex.EncodeToString(key)] = hex.EncodeToString(val)
		iter.Next()
	}

	return volumes
}
