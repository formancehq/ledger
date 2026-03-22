package state

import (
	"fmt"
	"math/big"

	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// ErrStalePreloadDetected is returned when a preload would seed the cache
// with a value that is older than what the cache already holds.
// This indicates a potential stale-overwrite bug.
type ErrStalePreloadDetected struct {
	ID            attributes.U128
	CacheInput    string
	CacheOutput   string
	PreloadInput  string
	PreloadOutput string
}

func (e *ErrStalePreloadDetected) Error() string {
	return fmt.Sprintf(
		"stale preload detected for volume %s: cache has input=%s output=%s, preload has input=%s output=%s",
		e.ID.Hex(), e.CacheInput, e.CacheOutput, e.PreloadInput, e.PreloadOutput,
	)
}

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
		"cache/pebble volume divergence for %s/%s/%s at raft index %d: cache(input=%s, output=%s) != pebble(input=%s, output=%s)",
		e.Key.Ledger, e.Key.Account, e.Key.Asset, e.RaftIndex,
		e.CacheInput, e.CacheOutput, e.PebbleInput, e.PebbleOutput,
	)
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
) error {
	for _, update := range volumeUpdates {
		// Read from Pebble (the committed value)
		pebbleValue, _, err := volumeAttr.ComputeValue(store, ^uint64(0), update.CanonicalKey)
		if err != nil {
			return fmt.Errorf("reading volume from pebble for verification: %w", err)
		}

		if pebbleValue == nil {
			return fmt.Errorf("volume missing from pebble after commit for %s/%s/%s at raft index %d",
				update.Key.Ledger, update.Key.Account, update.Key.Asset, raftIndex)
		}

		// Compare Pebble value with the expected value from Merge
		pebbleInput := pebbleValue.GetInput().ToBigInt()
		pebbleOutput := pebbleValue.GetOutput().ToBigInt()
		expectedInput := update.New.GetInput().ToBigInt()
		expectedOutput := update.New.GetOutput().ToBigInt()

		if pebbleInput.Cmp(expectedInput) != 0 || pebbleOutput.Cmp(expectedOutput) != 0 {
			assert.Unreachable("cache pebble volume divergence", map[string]any{
				"ledger":         update.Key.Ledger,
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
				"volume update has no old value for %s/%s/%s (preload missing)",
				update.Key.Ledger, update.Key.Account, update.Key.Asset,
			)
		}

		old := update.Old.Value()
		if old == nil {
			return fmt.Errorf(
				"volume update has nil old value for %s/%s/%s (preload missing)",
				update.Key.Ledger, update.Key.Account, update.Key.Asset,
			)
		}

		oldInput := old.GetInput().ToBigInt()
		oldOutput := old.GetOutput().ToBigInt()
		newInput := update.New.GetInput().ToBigInt()
		newOutput := update.New.GetOutput().ToBigInt()

		if newInput.Cmp(oldInput) < 0 {
			assert.Unreachable("volume input decreased", map[string]any{
				"ledger":   update.Key.Ledger,
				"account":  update.Key.Account,
				"asset":    update.Key.Asset,
				"oldInput": oldInput.String(),
				"newInput": newInput.String(),
			})

			return fmt.Errorf(
				"volume input decreased for %s/%s/%s: old=%s, new=%s (stale base value suspected)",
				update.Key.Ledger, update.Key.Account, update.Key.Asset,
				oldInput.String(), newInput.String(),
			)
		}

		if newOutput.Cmp(oldOutput) < 0 {
			assert.Unreachable("volume output decreased", map[string]any{
				"ledger":    update.Key.Ledger,
				"account":   update.Key.Account,
				"asset":     update.Key.Asset,
				"oldOutput": oldOutput.String(),
				"newOutput": newOutput.String(),
			})

			return fmt.Errorf(
				"volume output decreased for %s/%s/%s: old=%s, new=%s (stale base value suspected)",
				update.Key.Ledger, update.Key.Account, update.Key.Asset,
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

		ledger := apply.Apply.GetLedgerName()
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

			srcKey := domain.VolumeKey{
				AccountKey: domain.AccountKey{Ledger: ledger, Account: posting.GetSource()},
				Asset:      posting.GetAsset(),
			}

			dstKey := domain.VolumeKey{
				AccountKey: domain.AccountKey{Ledger: ledger, Account: posting.GetDestination()},
				Asset:      posting.GetAsset(),
			}

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
				"volume delta computation has no old value for %s/%s/%s (preload missing)",
				update.Key.Ledger, update.Key.Account, update.Key.Asset,
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
				"volume delta missing for %s/%s/%s: expected input_delta=%s output_delta=%s",
				key.Ledger, key.Account, key.Asset, exp.input.String(), exp.output.String(),
			)
		}

		if exp.input.Cmp(act.input) != 0 || exp.output.Cmp(act.output) != 0 {
			return fmt.Errorf(
				"volume delta mismatch for %s/%s/%s: expected(input_delta=%s, output_delta=%s), actual(input_delta=%s, output_delta=%s)",
				key.Ledger, key.Account, key.Asset,
				exp.input.String(), exp.output.String(),
				act.input.String(), act.output.String(),
			)
		}
	}

	return nil
}

// collectLedgerNames extracts unique ledger names from proposal orders.
func collectLedgerNames(orders []*raftcmdpb.Order) []string {
	seen := make(map[string]struct{})

	for _, order := range orders {
		switch {
		case order.GetCreateLedger() != nil:
			seen[order.GetCreateLedger().GetName()] = struct{}{}
		case order.GetApply() != nil:
			seen[order.GetApply().GetLedger()] = struct{}{}
		}
	}

	names := make([]string, 0, len(seen))
	for name := range seen {
		if name != "" {
			names = append(names, name)
		}
	}

	return names
}

// verifyAggregatedVolumesBalanced checks that for every ledger touched by the
// current proposal, the global aggregated volumes satisfy input == output per
// asset (double-entry invariant). This is a heavy but thorough check that
// catches any volume corruption regardless of root cause.
func verifyAggregatedVolumesBalanced(
	store *dal.Store,
	volumeAttr *attributes.Attribute[*raftcmdpb.VolumePair],
	ledgerNames []string,
	raftIndex uint64,
) error {
	for _, ledger := range ledgerNames {
		result, err := query.AggregateAllVolumes(store, volumeAttr, ledger, false)
		if err != nil {
			return fmt.Errorf("aggregating volumes for ledger %s at raft index %d: %w", ledger, raftIndex, err)
		}

		for _, vol := range result.GetVolumes() {
			inputVal := vol.GetInput().ToBigInt()
			outputVal := vol.GetOutput().ToBigInt()

			if inputVal.Cmp(outputVal) != 0 {
				assert.Unreachable("aggregated volume imbalance", map[string]any{
					"ledger":    ledger,
					"asset":     vol.GetAsset(),
					"raftIndex": raftIndex,
					"input":     inputVal.String(),
					"output":    outputVal.String(),
				})

				return fmt.Errorf(
					"aggregated volume imbalance for ledger %s asset %s at raft index %d: input=%s output=%s",
					ledger, vol.GetAsset(), raftIndex, inputVal.String(), outputVal.String(),
				)
			}
		}
	}

	return nil
}
