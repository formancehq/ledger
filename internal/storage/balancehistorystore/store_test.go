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

func inputEffect(audit, log, effective, inserted uint64, ledgerName, account string, amount uint64) balancehistory.Effect {
	return balancehistory.Effect{
		LedgerName:     ledgerName,
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

func outputEffect(audit, log, effective, inserted uint64, ledgerName, account string, amount uint64) balancehistory.Effect {
	effect := inputEffect(audit, log, effective, inserted, ledgerName, account, amount)
	effect.Input = balancehistory.Amount{}
	effect.Output = balancehistory.AmountFromUint64(amount)

	return effect
}

func publishBalanced(t *testing.T, store *Store, audit, log, effective, inserted uint64, amount uint64) Manifest {
	t.Helper()

	manifest, err := store.Publish(Publication{
		Effects: []balancehistory.Effect{
			inputEffect(audit, log, effective, inserted, "default", "assets:cash", amount),
			outputEffect(audit, log, effective, inserted, "default", "world", amount),
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

	oldVolumes, err := oldView.ReadVolumes("default", TemporalityEffective, 10, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, oldVolumes, 1)
	require.Equal(t, "5", oldVolumes[0].Input.String())

	newVolumes, err := newView.ReadVolumes("default", TemporalityEffective, 10, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, newVolumes, 1)
	require.Equal(t, "8", newVolumes[0].Input.String())

	insertionBeforeCorrection, err := newView.ReadVolumes("default", TemporalityInsertion, 150, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, insertionBeforeCorrection, 1)
	require.Equal(t, "5", insertionBeforeCorrection[0].Input.String())

	insertionAfterCorrection, err := newView.ReadVolumes("default", TemporalityInsertion, 250, []string{"assets:cash"})
	require.NoError(t, err)
	require.Len(t, insertionAfterCorrection, 1)
	require.Equal(t, "8", insertionAfterCorrection[0].Input.String())

	assets, err := newView.ReadVolumes("default", TemporalityEffective, 10, nil)
	require.NoError(t, err)
	require.Len(t, assets, 2)

	require.NoError(t, store.Verify())
}

func TestStoreLedgerNamesAreIsolated(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	_, err := store.Publish(Publication{
		Effects: []balancehistory.Effect{
			inputEffect(1, 1, 10, 10, "first", "same:name", 4),
			inputEffect(1, 1, 10, 10, "second", "same:name", 9),
		},
		Coverage: Coverage{AuditSequence: 1, LogSequence: 1, SourceComplete: true},
	})
	require.NoError(t, err)

	view, err := store.OpenView(1)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	first, err := view.ReadVolumes("first", TemporalityEffective, 10, nil)
	require.NoError(t, err)
	require.Len(t, first, 1)
	require.Equal(t, "4", first[0].Input.String())

	second, err := view.ReadVolumes("second", TemporalityEffective, 10, nil)
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
				LedgerName: "default", AuditSequence: audit, LogSequence: audit,
				EffectiveAt: audit, InsertedAt: audit, Account: "a", AssetBase: "USD", Input: amount,
			}},
			Coverage: Coverage{AuditSequence: audit, LogSequence: audit, SourceComplete: true},
		})
		require.NoError(t, err)
	}

	view, err := store.OpenView(2)
	require.NoError(t, err)
	defer func() { require.NoError(t, view.Close()) }()

	volumes, err := view.ReadVolumes("default", TemporalityEffective, math.MaxUint64, nil)
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

	run := manifest.Segments[0]
	identity := recordIdentity{Temporality: TemporalityEffective, LedgerName: "default", Account: "assets:cash", AssetBase: "USD", AssetPrecision: 2}
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
	require.Empty(t, manifest.Segments)

	_, err = store.OpenView(1)
	var building *ErrBuilding
	require.True(t, errors.As(err, &building))
}

func TestStoreRejectsSourceGap(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	publishBalanced(t, store, 2, 2, 10, 10, 1)

	_, err := store.Publish(Publication{
		Effects:  []balancehistory.Effect{inputEffect(2, 2, 10, 10, "default", "a", 1)},
		Coverage: Coverage{AuditSequence: 2, LogSequence: 2, SourceComplete: true},
	})
	var gap *ErrSourceGap
	require.ErrorAs(t, err, &gap)
}

func TestStoreRequiresCompleteSourceAndDefaultsToExactZeroFloor(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	_, err := store.Publish(Publication{
		Effects:  []balancehistory.Effect{inputEffect(1, 1, 10, 20, "default", "a", 1)},
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

	volumes, err := view.ReadVolumes("default", TemporalityEffective, 9, nil)
	require.NoError(t, err)
	require.Empty(t, volumes)

	assets, err := view.ReadVolumes("default", TemporalityInsertion, 19, nil)
	require.NoError(t, err)
	require.Empty(t, assets)

	volumes, err = view.ReadVolumes("default", TemporalityEffective, 10, nil)
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
	require.Len(t, pinnedManifest.Segments, 4)

	compacted, err := store.Compact(4)
	require.NoError(t, err)
	require.True(t, compacted)
	require.NoError(t, store.Verify())

	latest, err := store.OpenView(4)
	require.NoError(t, err)
	defer func() { require.NoError(t, latest.Close()) }()
	require.Len(t, latest.Manifest().Segments, 1)
	require.Equal(t, uint32(1), latest.Manifest().Segments[0].Level)

	for _, view := range []*View{pinned, latest} {
		volumes, err := view.ReadVolumes("default", TemporalityEffective, 4, []string{"assets:cash"})
		require.NoError(t, err)
		require.Len(t, volumes, 1)
		require.Equal(t, "10", volumes[0].Input.String())

		assets, err := view.ReadVolumes("default", TemporalityInsertion, 4, nil)
		require.NoError(t, err)
		require.Len(t, assets, 2)
	}
}
