package state

import (
	"encoding/binary"
	"fmt"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/cockroachdb/pebble/v2"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/cache"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ErrVolumeCachePebbleDivergence is returned when the cache volume does not
// match what was persisted to Pebble, indicating a cache/storage inconsistency.
type ErrVolumeCachePebbleDivergence struct {
	Key          domain.VolumeKey
	CacheInput   string
	CacheOutput  string
	PebbleInput  string
	PebbleOutput string
	RaftIndex    uint64
}

func (e *ErrVolumeCachePebbleDivergence) Error() string {
	return fmt.Sprintf(
		"cache/pebble volume divergence for %d/%s/%s at raft index %d: cache(input=%s, output=%s) != pebble(input=%s, output=%s)",
		e.Key.LedgerID, e.Key.Account, e.Key.Asset, e.RaftIndex,
		e.CacheInput, e.CacheOutput, e.PebbleInput, e.PebbleOutput,
	)
}

// deduplicateVolumeUpdates collects volume updates from all ApplyResults and
// keeps only the latest update per canonical key. When multiple raft entries in
// the same ApplyEntries batch modify the same volume, only the last entry's
// value is persisted in Pebble (earlier entries are deleted by mergeSimple's
// DeleteAt). Results are iterated in order so later entries naturally overwrite
// earlier ones.
//
// Volumes purged by ephemeral purge in a later entry are excluded: the purge
// deletes the Pebble entry written by the earlier entry, so verifying the
// earlier entry's expected value would fail with "volume missing from pebble".
func deduplicateVolumeUpdates(results []ApplyResult) []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair] {
	seen := make(map[domain.VolumeKey]int) // key -> index in deduped slice
	var deduped []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]

	for _, r := range results {
		for _, update := range r.volumeUpdates {
			if idx, ok := seen[update.Key]; ok {
				deduped[idx] = update
			} else {
				seen[update.Key] = len(deduped)
				deduped = append(deduped, update)
			}
		}

		// Remove volumes that were purged by this entry's ephemeral purge.
		// A purge deletes the Pebble entry, which may have been written by
		// an earlier entry in this same batch.
		for _, key := range r.purgedVolumeKeys {
			if idx, ok := seen[key]; ok {
				// Mark as removed by setting to zero value; compact below.
				deduped[idx] = attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair]{}
				delete(seen, key)
			}
		}
	}

	// Compact: remove zero-value entries left by purge removals.
	compact := deduped[:0]
	for _, u := range deduped {
		if len(u.CanonicalKey) > 0 {
			compact = append(compact, u)
		}
	}

	return compact
}

// verifyPostCommitVolumes reads back volumes from Pebble after batch commit
// and compares them with the expected values from the Merge (update.New).
// This catches bugs where Pebble diverges from what the FSM intended to write.
//
// We use update.New (the value written during Merge) instead of reading from
// the cache because cache generation rotations during a batch can evict entries
// before this verification runs (e.g., during replay of large batches spanning
// multiple generation thresholds).
func verifyPostCommitVolumes(
	store *dal.Store,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	volumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	raftIndex uint64,
	logger logging.Logger,
) error {
	for _, update := range volumeUpdates {
		// Read from Pebble (the committed value)
		pebbleValue, err := volumeAttr.Get(store, update.CanonicalKey)
		if err != nil {
			return fmt.Errorf("reading volume from pebble for verification: %w", err)
		}

		if pebbleValue == nil {
			logger.WithFields(map[string]any{
				"ledger":       update.Key.LedgerID,
				"account":      update.Key.Account,
				"asset":        update.Key.Asset,
				"raftIndex":    raftIndex,
				"canonicalKey": fmt.Sprintf("%x", update.CanonicalKey),
				"id":           fmt.Sprintf("%x", update.ID),
			}).Errorf("SENTINEL DIAG: volume missing from pebble after commit")

			return fmt.Errorf("volume missing from pebble after commit for %d/%s/%s at raft index %d (canonicalKey=%x)",
				update.Key.LedgerID, update.Key.Account, update.Key.Asset, raftIndex, update.CanonicalKey)
		}

		// Compare Pebble value with the expected value from Merge
		pebbleInput := pebbleValue.GetInput().ToBigInt()
		pebbleOutput := pebbleValue.GetOutput().ToBigInt()
		expectedInput := update.New.GetInput().ToBigInt()
		expectedOutput := update.New.GetOutput().ToBigInt()

		if pebbleInput.Cmp(expectedInput) != 0 || pebbleOutput.Cmp(expectedOutput) != 0 {
			// Log full diagnostic before asserting
			logger.WithFields(map[string]any{
				"ledger":         update.Key.LedgerID,
				"account":        update.Key.Account,
				"asset":          update.Key.Asset,
				"expectedInput":  expectedInput.String(),
				"expectedOutput": expectedOutput.String(),
				"pebbleInput":    pebbleInput.String(),
				"pebbleOutput":   pebbleOutput.String(),
				"raftIndex":      raftIndex,
				"canonicalKey":   fmt.Sprintf("%x", update.CanonicalKey),
				"id":             fmt.Sprintf("%x", update.ID),
			}).Errorf("SENTINEL DIAG: cache/pebble volume divergence")

			assert.Unreachable("cache pebble volume divergence", map[string]any{
				"ledger":         update.Key.LedgerID,
				"account":        update.Key.Account,
				"asset":          update.Key.Asset,
				"expectedInput":  expectedInput.String(),
				"expectedOutput": expectedOutput.String(),
				"pebbleInput":    pebbleInput.String(),
				"pebbleOutput":   pebbleOutput.String(),
				"raftIndex":      raftIndex,
			})

			return &ErrVolumeCachePebbleDivergence{
				Key:          update.Key,
				CacheInput:   expectedInput.String(),
				CacheOutput:  expectedOutput.String(),
				PebbleInput:  pebbleInput.String(),
				PebbleOutput: pebbleOutput.String(),
				RaftIndex:    raftIndex,
			}
		}
	}

	return nil
}

// verifyVolumeUpdateMonotonicity checks that volume updates are monotonically
// increasing (input and output can only grow, never shrink). A shrinking volume
// indicates a stale base value was used during processing.
func verifyVolumeUpdateMonotonicity(
	volumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
) error {
	for _, update := range volumeUpdates {
		if !update.Old.IsDefined() {
			return fmt.Errorf(
				"volume update has no old value for %d/%s/%s (preload missing)",
				update.Key.LedgerID, update.Key.Account, update.Key.Asset,
			)
		}

		old := update.Old.Value()
		if old == nil {
			return fmt.Errorf(
				"volume update has nil old value for %d/%s/%s (preload missing)",
				update.Key.LedgerID, update.Key.Account, update.Key.Asset,
			)
		}

		oldInput := old.GetInput().ToBigInt()
		oldOutput := old.GetOutput().ToBigInt()
		newInput := update.New.GetInput().ToBigInt()
		newOutput := update.New.GetOutput().ToBigInt()

		if newInput.Cmp(oldInput) < 0 {
			assert.Unreachable("volume input decreased", map[string]any{
				"ledger":   update.Key.LedgerID,
				"account":  update.Key.Account,
				"asset":    update.Key.Asset,
				"oldInput": oldInput.String(),
				"newInput": newInput.String(),
			})

			return fmt.Errorf(
				"volume input decreased for %d/%s/%s: old=%s, new=%s (stale base value suspected)",
				update.Key.LedgerID, update.Key.Account, update.Key.Asset,
				oldInput.String(), newInput.String(),
			)
		}

		if newOutput.Cmp(oldOutput) < 0 {
			assert.Unreachable("volume output decreased", map[string]any{
				"ledger":    update.Key.LedgerID,
				"account":   update.Key.Account,
				"asset":     update.Key.Asset,
				"oldOutput": oldOutput.String(),
				"newOutput": newOutput.String(),
			})

			return fmt.Errorf(
				"volume output decreased for %d/%s/%s: old=%s, new=%s (stale base value suspected)",
				update.Key.LedgerID, update.Key.Account, update.Key.Asset,
				oldOutput.String(), newOutput.String(),
			)
		}
	}

	return nil
}

// verifyVolumeDeltasMatchPostings cross-checks that the volume deltas produced
// by buffer processing match what the postings in the committed logs prescribe.
// This catches bugs where volumes are updated incorrectly (wrong amount, wrong
// account, or missed posting).
func verifyVolumeDeltasMatchPostings(
	volumeUpdates []attributes.Update[domain.VolumeKey, *raftcmdpb.VolumePair],
	logs []*commonpb.Log,
	ledgerNameToID map[string]uint32,
) error {
	// Compute expected deltas from postings
	type delta struct {
		input  *big.Int
		output *big.Int
	}

	expected := make(map[domain.VolumeKey]*delta)

	for _, log := range logs {
		if log.GetPayload() == nil {
			continue
		}

		apply, ok := log.GetPayload().GetType().(*commonpb.LogPayload_Apply)
		if !ok || apply.Apply == nil || apply.Apply.GetLog() == nil || apply.Apply.GetLog().GetData() == nil {
			continue
		}

		ledgerName := apply.Apply.GetLedgerName()
		ledgerID := ledgerNameToID[ledgerName]
		data := apply.Apply.GetLog().GetData()

		var postings []*commonpb.Posting

		switch p := data.GetPayload().(type) {
		case *commonpb.LedgerLogPayload_CreatedTransaction:
			if p.CreatedTransaction != nil && p.CreatedTransaction.GetTransaction() != nil {
				postings = p.CreatedTransaction.GetTransaction().GetPostings()
			}
		case *commonpb.LedgerLogPayload_RevertedTransaction:
			if p.RevertedTransaction != nil && p.RevertedTransaction.GetRevertTransaction() != nil {
				postings = p.RevertedTransaction.GetRevertTransaction().GetPostings()
			}
		}

		for _, posting := range postings {
			amount := posting.GetAmount().ToBigInt()

			srcKey := domain.NewVolumeKey(ledgerID, posting.GetSource(), posting.GetAsset())
			dstKey := domain.NewVolumeKey(ledgerID, posting.GetDestination(), posting.GetAsset())

			if _, ok := expected[srcKey]; !ok {
				expected[srcKey] = &delta{input: big.NewInt(0), output: big.NewInt(0)}
			}

			expected[srcKey].output.Add(expected[srcKey].output, amount)

			if _, ok := expected[dstKey]; !ok {
				expected[dstKey] = &delta{input: big.NewInt(0), output: big.NewInt(0)}
			}

			expected[dstKey].input.Add(expected[dstKey].input, amount)
		}
	}

	// Compute actual deltas from volume updates
	actual := make(map[domain.VolumeKey]*delta)

	for _, update := range volumeUpdates {
		newInput := update.New.GetInput().ToBigInt()
		newOutput := update.New.GetOutput().ToBigInt()

		var oldInput, oldOutput *big.Int

		if !update.Old.IsDefined() || update.Old.Value() == nil {
			return fmt.Errorf(
				"volume delta computation has no old value for %d/%s/%s (preload missing)",
				update.Key.LedgerID, update.Key.Account, update.Key.Asset,
			)
		}

		oldInput = update.Old.Value().GetInput().ToBigInt()
		oldOutput = update.Old.Value().GetOutput().ToBigInt()

		inputDelta := new(big.Int).Sub(newInput, oldInput)
		outputDelta := new(big.Int).Sub(newOutput, oldOutput)

		actual[update.Key] = &delta{input: inputDelta, output: outputDelta}
	}

	// Compare expected vs actual
	for key, exp := range expected {
		act, ok := actual[key]
		if !ok {
			return fmt.Errorf(
				"volume delta missing for %d/%s/%s: expected input_delta=%s output_delta=%s",
				key.LedgerID, key.Account, key.Asset, exp.input.String(), exp.output.String(),
			)
		}

		if exp.input.Cmp(act.input) != 0 || exp.output.Cmp(act.output) != 0 {
			return fmt.Errorf(
				"volume delta mismatch for %d/%s/%s: expected(input_delta=%s, output_delta=%s), actual(input_delta=%s, output_delta=%s)",
				key.LedgerID, key.Account, key.Asset,
				exp.input.String(), exp.output.String(),
				act.input.String(), act.output.String(),
			)
		}
	}

	return nil
}

// collectLedgerIDs extracts unique ledger IDs from proposal orders,
// resolving names to IDs via the store.
func collectLedgerIDs(orders []*raftcmdpb.Order, store processing.InMemoryStore) []uint32 {
	seen := make(map[uint32]struct{})

	for _, order := range orders {
		var name string
		switch {
		case order.GetCreateLedger() != nil:
			name = order.GetCreateLedger().GetName()
		case order.GetApply() != nil:
			name = order.GetApply().GetLedger()
		}

		if name == "" {
			continue
		}

		info, ok := store.GetLedger(name)
		if ok {
			seen[info.GetId()] = struct{}{}
		}
	}

	ids := make([]uint32, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}

	return ids
}

// collectLedgerIDsFromResults extracts unique ledger IDs from all
// ApplyResults in the batch. Used for post-commit aggregated balance checks.
func collectLedgerIDsFromResults(results []ApplyResult) []uint32 {
	seen := make(map[uint32]struct{})

	for _, r := range results {
		for _, id := range r.ledgerIDs {
			seen[id] = struct{}{}
		}
	}

	ids := make([]uint32, 0, len(seen))
	for id := range seen {
		ids = append(ids, id)
	}

	return ids
}

// verifyAggregatedVolumesBalanced checks that for every ledger touched by the
// current proposal, the global aggregated volumes satisfy input == output per
// asset (double-entry invariant). This is a heavy but thorough check that
// catches any volume corruption regardless of root cause.
func verifyAggregatedVolumesBalanced(
	store *dal.Store,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	ledgerIDs []uint32,
	raftIndex uint64,
	logger logging.Logger,
) error {
	for _, ledgerID := range ledgerIDs {
		result, err := query.AggregateAllVolumes(store, volumeAttr, ledgerID, query.AggregateOptions{})
		if err != nil {
			return fmt.Errorf("aggregating volumes for ledger %d at raft index %d: %w", ledgerID, raftIndex, err)
		}

		for _, vol := range result.GetVolumes() {
			inputVal := vol.GetInput().ToBigInt()
			outputVal := vol.GetOutput().ToBigInt()

			if inputVal.Cmp(outputVal) != 0 {
				// Dump per-account volumes for the imbalanced asset to help
				// identify which account(s) are responsible.
				dumpPerAccountVolumes(store, volumeAttr, ledgerID, vol.GetAsset(), raftIndex, logger)

				assert.Unreachable("aggregated volume imbalance", map[string]any{
					"ledger":    ledgerID,
					"asset":     vol.GetAsset(),
					"raftIndex": raftIndex,
					"input":     inputVal.String(),
					"output":    outputVal.String(),
				})

				return fmt.Errorf(
					"aggregated volume imbalance for ledger %d asset %s at raft index %d: input=%s output=%s",
					ledgerID, vol.GetAsset(), raftIndex, inputVal.String(), outputVal.String(),
				)
			}

			// Log aggregated totals for every checked ledger/asset.
			// This traces the exact entry where cumulative volumes diverge.
			logger.WithFields(map[string]any{
				"ledger":    ledgerID,
				"asset":     vol.GetAsset(),
				"raftIndex": raftIndex,
				"input":     inputVal.String(),
				"output":    outputVal.String(),
			}).Infof("SENTINEL: aggregated volume OK")
		}
	}

	return nil
}

// dumpPerAccountVolumes logs every individual account volume for the given
// ledger and asset. Called when an aggregated imbalance is detected.
func dumpPerAccountVolumes(
	store *dal.Store,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	ledgerID uint32,
	asset string,
	raftIndex uint64,
	logger logging.Logger,
) {
	ledgerPrefix := make([]byte, 4)
	binary.BigEndian.PutUint32(ledgerPrefix, ledgerID)

	iter, err := volumeAttr.NewStreamingIter(store, ledgerPrefix)
	if err != nil {
		logger.Errorf("dumpPerAccountVolumes: failed to create iterator: %v", err)

		return
	}

	var count int

	for iter.Next() {
		entry := iter.Entry()

		var vk domain.VolumeKey
		if err := vk.Unmarshal(entry.CanonicalKey); err != nil {
			logger.Errorf("dumpPerAccountVolumes: unmarshal error: %v", err)

			continue
		}

		if vk.Asset != asset {
			continue
		}

		inputVal := entry.Value.GetInput().ToBigInt()
		outputVal := entry.Value.GetOutput().ToBigInt()

		logger.WithFields(map[string]any{
			"ledger":       ledgerID,
			"account":      vk.Account,
			"asset":        vk.Asset,
			"input":        inputVal.String(),
			"output":       outputVal.String(),
			"canonicalKey": fmt.Sprintf("%x", entry.CanonicalKey),
			"raftIndex": raftIndex,
		}).Errorf("VOLUME DUMP: per-account volume at imbalance")

		count++
	}

	if err := iter.Close(); err != nil {
		logger.Errorf("dumpPerAccountVolumes: close error: %v", err)
	}

	if err := iter.Err(); err != nil {
		logger.Errorf("dumpPerAccountVolumes: iteration error: %v", err)
	}

	logger.WithFields(map[string]any{
		"ledger":    ledgerID,
		"asset":     asset,
		"raftIndex": raftIndex,
		"count":     count,
	}).Errorf("VOLUME DUMP: total accounts dumped for imbalanced asset")
}

// dumpCacheVsPebbleCoherence compares the in-memory volume cache (gen0 + gen1)
// against the 0xFF Pebble cache zone. Logs the sizes and any keys that are in
// one but not the other. Called only on sentinel check failure.
func dumpCacheVsPebbleCoherence(
	store dal.PebbleReader,
	c *cache.Cache,
	raftIndex uint64,
	logger logging.Logger,
) {
	currentGen := c.CurrentGeneration()
	gen0Byte := byte(currentGen % 2)
	gen1Byte := byte((currentGen + 1) % 2)

	// Count in-memory entries
	memGen0 := c.Volumes.Gen0().Size()
	memGen1 := c.Volumes.Gen1().Size()

	// Count 0xFF entries for each gen byte
	pebbleGen0 := countCacheZoneEntries(store, gen0Byte, dal.SubAttrVolume)
	pebbleGen1 := countCacheZoneEntries(store, gen1Byte, dal.SubAttrVolume)

	// Count 0xF1 attribute entries (ground truth)
	pebbleAttr := countAttributeEntries(store, dal.SubAttrVolume)

	logger.WithFields(map[string]any{
		"raftIndex":         raftIndex,
		"currentGeneration": currentGen,
		"gen0Base":          c.BaseIndex.Gen0,
		"gen1Base":          c.BaseIndex.Gen1,
		"memGen0":           memGen0,
		"memGen1":           memGen1,
		"pebbleGen0_0xFF":   pebbleGen0,
		"pebbleGen1_0xFF":   pebbleGen1,
		"pebbleAttr_0xF1":   pebbleAttr,
		"memTotal":          memGen0 + memGen1,
		"pebbleTotal_0xFF":  pebbleGen0 + pebbleGen1,
	}).Errorf("CACHE COHERENCE: volume cache vs Pebble sizes")

	// Find keys in memory but NOT in 0xFF (the smoking gun for the bug)
	inMemNotInPebble := 0

	for id := range c.Volumes.Gen0().Iter() {
		if !cacheZoneHasKey(store, gen0Byte, dal.SubAttrVolume, id) {
			inMemNotInPebble++
			if inMemNotInPebble <= 10 {
				logger.WithFields(map[string]any{
					"id":  fmt.Sprintf("%x", id),
					"gen": "gen0",
				}).Errorf("CACHE COHERENCE: key in memory gen0 but NOT in 0xFF")
			}
		}
	}

	for id := range c.Volumes.Gen1().Iter() {
		if !cacheZoneHasKey(store, gen1Byte, dal.SubAttrVolume, id) {
			inMemNotInPebble++
			if inMemNotInPebble <= 10 {
				logger.WithFields(map[string]any{
					"id":  fmt.Sprintf("%x", id),
					"gen": "gen1",
				}).Errorf("CACHE COHERENCE: key in memory gen1 but NOT in 0xFF")
			}
		}
	}

	logger.WithFields(map[string]any{
		"inMemNotInPebble": inMemNotInPebble,
	}).Errorf("CACHE COHERENCE: summary (keys in memory but missing from 0xFF)")
}

func countCacheZoneEntries(store dal.PebbleReader, genByte, cacheType byte) uint64 {
	lower := []byte{dal.ZoneCache, genByte, cacheType}
	upper := []byte{dal.ZoneCache, genByte, cacheType + 1}

	iter, err := store.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0
	}

	var count uint64
	for iter.First(); iter.Valid(); iter.Next() {
		if len(iter.Key()) >= 3+16 {
			count++
		}
	}

	_ = iter.Close()

	return count
}

func countAttributeEntries(store dal.PebbleReader, attrType byte) uint64 {
	lower := []byte{dal.ZoneAttributes, attrType}
	upper := []byte{dal.ZoneAttributes, attrType + 1}

	iter, err := store.NewIter(&pebble.IterOptions{
		LowerBound: lower,
		UpperBound: upper,
	})
	if err != nil {
		return 0
	}

	var count uint64
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	_ = iter.Close()

	return count
}

func cacheZoneHasKey(store dal.PebbleReader, genByte, cacheType byte, id attributes.U128) bool {
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
