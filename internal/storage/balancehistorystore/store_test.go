package balancehistorystore

import (
	"context"
	"errors"
	"math"
	"math/big"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
)

func newTestStore(t *testing.T) *Store {
	t.Helper()

	store, err := New(t.TempDir(), logging.NopZap(), DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, store.Close()) })

	return store
}

func inputEffect(audit, log, effective, inserted uint64, ledgerID uint32, account string, amount uint64) balancehistory.Effect {
	return balancehistory.Effect{
		LedgerID:       ledgerID,
		AuditSequence:  audit,
		LogSequence:    log,
		EffectiveAt:    effective,
		InsertedAt:     inserted,
		Account:        account,
		AssetBase:      "USD",
		AssetPrecision: 2,
		Input:          balancehistory.AmountFromUint64(amount),
	}
}

func outputEffect(audit, log, effective, inserted uint64, ledgerID uint32, account string, amount uint64) balancehistory.Effect {
	effect := inputEffect(audit, log, effective, inserted, ledgerID, account, amount)
	effect.Input = balancehistory.Amount{}
	effect.Output = balancehistory.AmountFromUint64(amount)

	return effect
}

func publishBalanced(t *testing.T, store *Store, audit, log, effective, inserted uint64, amount uint64) Manifest {
	t.Helper()

	manifest, err := store.Publish(Publication{
		Effects: []balancehistory.Effect{
			inputEffect(audit, log, effective, inserted, 7, "assets:cash", amount),
			outputEffect(audit, log, effective, inserted, 7, "world", amount),
		},
		Coverage: Coverage{AuditSequence: audit, LogSequence: log, AuditHash: []byte{byte(audit)}, SourceComplete: true},
	})
	require.NoError(t, err)

	return manifest
}

func TestStoreEffectiveAndInsertionViews(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	publishBalanced(t, store, 1, 1, 10, 100, 5)

	oldView, err := store.OpenView(1)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, oldView.Close()) })

	// Inserted later, but effective earlier: it restates the effective axis and
	// leaves an older insertion-time result unchanged.
	publishBalanced(t, store, 2, 2, 5, 200, 3)

	newView, err := store.OpenView(2)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, newView.Close()) })

	oldVolumes, err := oldView.ReadVolumes(7, AxisEffective, 10, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, oldVolumes, 1)
	require.Equal(t, "5", oldVolumes[0].Input.String())

	newVolumes, err := newView.ReadVolumes(7, AxisEffective, 10, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, newVolumes, 1)
	require.Equal(t, "8", newVolumes[0].Input.String())

	insertionBeforeCorrection, err := newView.ReadVolumes(7, AxisInsertion, 150, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, insertionBeforeCorrection, 1)
	require.Equal(t, "5", insertionBeforeCorrection[0].Input.String())

	insertionAfterCorrection, err := newView.ReadVolumes(7, AxisInsertion, 250, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, insertionAfterCorrection, 1)
	require.Equal(t, "8", insertionAfterCorrection[0].Input.String())

	assets, err := newView.AggregateAll(7, AxisEffective, 10)
	require.NoError(t, err)
	require.Len(t, assets, 1)
	require.Equal(t, "8", assets[0].Input.String())
	require.Equal(t, "8", assets[0].Output.String())

	require.NoError(t, store.Verify())
}

func TestStoreLedgerIncarnationsAreIsolated(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	_, err := store.Publish(Publication{
		Effects: []balancehistory.Effect{
			inputEffect(1, 1, 10, 10, 7, "same:name", 4),
			inputEffect(1, 1, 10, 10, 8, "same:name", 9),
		},
		Coverage: Coverage{AuditSequence: 1, LogSequence: 1, SourceComplete: true},
	})
	require.NoError(t, err)

	view, err := store.OpenView(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	first, err := view.ReadVolumes(7, AxisEffective, 10, nil)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Equal(t, "4", first[0].Input.String())

	second, err := view.ReadVolumes(8, AxisEffective, 10, nil)
	require.NoError(t, err)
	require.Len(t, second, 1)
	require.Equal(t, "9", second[0].Input.String())
}

func TestStoreKeepsArbitraryPrecisionSummaries(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	maxAmount := new(big.Int).Sub(new(big.Int).Lsh(big.NewInt(1), 256), big.NewInt(1))
	var amount balancehistory.Amount
	maxAmount.FillBytes(amount[:])

	for audit := uint64(1); audit <= 2; audit++ {
		_, err := store.Publish(Publication{
			Effects: []balancehistory.Effect{{
				LedgerID: 1, AuditSequence: audit, LogSequence: audit,
				EffectiveAt: audit, InsertedAt: audit, Account: "a", AssetBase: "USD", Input: amount,
			}},
			Coverage: Coverage{AuditSequence: audit, LogSequence: audit, SourceComplete: true},
		})
		require.NoError(t, err)
	}

	view, err := store.OpenView(2)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	volumes, err := view.ReadVolumes(1, AxisEffective, math.MaxUint64, nil)
	require.NoError(t, err)
	require.Len(t, volumes, 1)
	require.Equal(t, 257, volumes[0].Input.BitLen())
}

func TestStoreFailsClosedOnLagAndCorruption(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	manifest := publishBalanced(t, store, 1, 1, 10, 10, 1)

	_, err := store.OpenView(2)
	var behind *ErrBehind
	require.ErrorAs(t, err, &behind)

	run := manifest.Runs[0]
	identity := recordIdentity{Axis: AxisEffective, Scope: scopeVolume, LedgerID: 7, Account: "assets:cash", AssetBase: "USD", AssetPrecision: 2}
	key, err := dataKey(run.ID, identity, 10)
	require.NoError(t, err)
	require.NoError(t, store.db.Set(key, []byte{0xff}, pebble.NoSync))

	var corrupt *ErrCorrupt
	require.ErrorAs(t, store.Verify(), &corrupt)
}

func TestStoreWaitAndReset(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- store.WaitForLogWatermark(ctx, 1) }()
	publishBalanced(t, store, 1, 1, 10, 10, 1)
	require.NoError(t, <-done)

	require.NoError(t, store.Reset())
	manifest, err := store.Manifest()
	require.NoError(t, err)
	require.Zero(t, manifest.Version)
	require.Empty(t, manifest.Runs)

	_, err = store.OpenView(1)
	var building *ErrBuilding
	require.True(t, errors.As(err, &building))
}

func TestStoreRejectsSourceGap(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	publishBalanced(t, store, 2, 2, 10, 10, 1)

	_, err := store.Publish(Publication{
		Effects:  []balancehistory.Effect{inputEffect(2, 2, 10, 10, 7, "a", 1)},
		Coverage: Coverage{AuditSequence: 2, LogSequence: 2, SourceComplete: true},
	})
	var gap *ErrSourceGap
	require.ErrorAs(t, err, &gap)
}

func TestStoreRequiresCompleteSourceAndDefaultsToExactZeroFloor(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	_, err := store.Publish(Publication{
		Effects:  []balancehistory.Effect{inputEffect(1, 1, 10, 20, 7, "a", 1)},
		Coverage: Coverage{AuditSequence: 1, LogSequence: 1},
	})
	require.NoError(t, err)

	_, err = store.OpenView(1)
	var building *ErrBuilding
	require.ErrorAs(t, err, &building)

	_, err = store.Publish(Publication{
		Coverage: Coverage{
			AuditSequence: 1, LogSequence: 1, SourceComplete: true,
		},
	})
	require.NoError(t, err)

	view, err := store.OpenView(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	volumes, err := view.ReadVolumes(7, AxisEffective, 9, nil)
	require.NoError(t, err)
	require.Empty(t, volumes)

	assets, err := view.AggregateAll(7, AxisInsertion, 19)
	require.NoError(t, err)
	require.Empty(t, assets)

	volumes, err = view.ReadVolumes(7, AxisEffective, 10, nil)
	require.NoError(t, err)
	require.Len(t, volumes, 1)
}

func TestCompactionPreservesResultsAndPinnedViews(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	for audit := uint64(1); audit <= 4; audit++ {
		publishBalanced(t, store, audit, audit, audit, audit, audit)
	}

	pinned, err := store.OpenView(4)
	require.NoError(t, err)
	defer func() { require.NoError(t, pinned.Close()) }()
	pinnedManifest := pinned.Manifest()
	require.Len(t, pinnedManifest.Runs, 4)

	compacted, err := store.Compact(4)
	require.NoError(t, err)
	require.True(t, compacted)
	require.NoError(t, store.Verify())

	latest, err := store.OpenView(4)
	require.NoError(t, err)
	defer func() { require.NoError(t, latest.Close()) }()
	require.Len(t, latest.Manifest().Runs, 1)
	require.Equal(t, uint32(1), latest.Manifest().Runs[0].Level)

	for _, view := range []*View{pinned, latest} {
		volumes, err := view.ReadVolumes(7, AxisEffective, 4, []string{"assets:cash"})
		require.NoError(t, err)
		require.Len(t, volumes, 1)
		require.Equal(t, "10", volumes[0].Input.String())

		assets, err := view.AggregateAll(7, AxisInsertion, 4)
		require.NoError(t, err)
		require.Len(t, assets, 1)
		require.Equal(t, "10", assets[0].Input.String())
		require.Equal(t, "10", assets[0].Output.String())
	}
}

func TestLogicalDigestIgnoresRunPartitioning(t *testing.T) {
	t.Parallel()

	separate := newTestStore(t)
	combined := newTestStore(t)

	first := []balancehistory.Effect{
		inputEffect(1, 1, 20, 100, 7, "a", 2),
		outputEffect(1, 1, 20, 100, 7, "world", 2),
	}
	second := []balancehistory.Effect{
		inputEffect(2, 2, 10, 200, 7, "a", 3),
		outputEffect(2, 2, 10, 200, 7, "world", 3),
	}

	_, err := separate.Publish(Publication{
		Effects:  first,
		Coverage: Coverage{AuditSequence: 1, LogSequence: 1, SourceComplete: true},
	})
	require.NoError(t, err)
	separateManifest, err := separate.Publish(Publication{
		Effects:  second,
		Coverage: Coverage{AuditSequence: 2, LogSequence: 2, SourceComplete: true},
	})
	require.NoError(t, err)

	combinedManifest, err := combined.Publish(Publication{
		Effects:  append(append([]balancehistory.Effect(nil), second...), first...),
		Coverage: Coverage{AuditSequence: 2, LogSequence: 2, SourceComplete: true},
	})
	require.NoError(t, err)

	require.Equal(t, separateManifest.LogicalDigest, combinedManifest.LogicalDigest)
	require.Len(t, separateManifest.Runs, 2)
	require.Len(t, combinedManifest.Runs, 1)
}
