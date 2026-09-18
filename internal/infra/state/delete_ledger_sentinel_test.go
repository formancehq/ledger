package state

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// A successful ledger deletion must not leave stale post-commit volume expectations.
func TestDeleteLedgerSentinel(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"same_proposal", "same_batch", "separate_batches"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			fsm, store, attrs := newTestMachine(t)
			fsm.sentinel = dal.NewSentinelFactory(store, true)
			fsm.sentinelMode = true
			ctx := context.Background()
			const ledger = "sentinel-delete"
			const survivor = "sentinel-delete-survivor"
			apply := func(entries ...*raftpb.Entry) {
				t.Helper()
				result, err := fsm.ApplyEntries(ctx, store, entries...)
				require.NoError(t, err, "intentional ledger deletion must keep the post-commit sentinel healthy")
				require.Len(t, result.Results, len(entries))
				for _, applied := range result.Results {
					require.NoError(t, applied.Error)
				}
			}
			apply(makeEntry(t, 1, makeProposal(1, createLedgerOrder(ledger), createLedgerOrder(survivor))))
			tx := createTransactionOrder(ledger, true, newPosting("world", "treasury", "EUR", 100))
			liveTx := createTransactionOrder(survivor, true, newPosting("world", "treasury", "EUR", 200))
			del := deleteLedgerOrder(ledger)
			switch mode {
			case "same_proposal":
				apply(makeEntry(t, 2, makeProposal(2, tx, liveTx, del)))
			case "same_batch":
				apply(makeEntry(t, 2, makeProposal(2, tx, liveTx)),
					makeEntry(t, 3, makeProposal(3, del)))
			case "separate_batches":
				apply(makeEntry(t, 2, makeProposal(2, tx, liveTx)))
				apply(makeEntry(t, 3, makeProposal(3, del)))
			}
			for _, account := range []string{"world", "treasury"} {
				pair, err := attrs.Volume.Get(store, domain.NewVolumeKey(ledger, account, "EUR", "").Bytes())
				require.NoError(t, err)
				require.Nil(t, pair, "ledger deletion physically removes the volume")
				pair, err = attrs.Volume.Get(store, domain.NewVolumeKey(survivor, account, "EUR", "").Bytes())
				require.NoError(t, err)
				require.NotNil(t, pair, "a ledger sharing the deleted name's prefix must survive")
				if account == "world" {
					require.Equal(t, "200", pair.GetOutput().ToBigInt().String())
					require.Zero(t, pair.GetInput().ToBigInt().Sign())
				} else {
					require.Equal(t, "200", pair.GetInput().ToBigInt().String())
					require.Zero(t, pair.GetOutput().ToBigInt().Sign())
				}
			}
			info, _, err := fsm.Registry.Ledgers.GetKey(domain.LedgerKey{Name: ledger})
			require.NoError(t, err)
			require.NotNil(t, info.GetDeletedAt(), "ledger was intentionally deleted")
		})
	}
}

// The WriteSet is reused for each entry. A later deletion must not overwrite
// an earlier ApplyResult's captured deletion names, nor remove surviving updates.
func TestDeleteLedgerSentinelMultipleDeletions(t *testing.T) {
	t.Parallel()
	fsm, store, attrs := newTestMachine(t)
	fsm.sentinel = dal.NewSentinelFactory(store, true)
	fsm.sentinelMode = true
	ctx := context.Background()
	_, err := fsm.ApplyEntries(ctx, store, makeEntry(t, 1, makeProposal(1,
		createLedgerOrder("first"), createLedgerOrder("second"), createLedgerOrder("live"))))
	require.NoError(t, err)

	pb, err := fsm.PrepareEntries(ctx, store,
		makeEntry(t, 2, makeProposal(2,
			createTransactionOrder("first", true, newPosting("world", "treasury", "EUR", 100)),
			createTransactionOrder("second", true, newPosting("world", "treasury", "EUR", 100)),
			createTransactionOrder("live", true, newPosting("world", "treasury", "EUR", 200)))),
		makeEntry(t, 3, makeProposal(3, deleteLedgerOrder("first"))),
		makeEntry(t, 4, makeProposal(4, deleteLedgerOrder("second"))),
		makeEntry(t, 5, makeProposal(5,
			createTransactionOrder("live", true, newPosting("world", "treasury", "EUR", 50)))))
	require.NoError(t, err)
	for _, result := range pb.Result.Results {
		require.NoError(t, result.Error)
	}
	require.NoError(t, fsm.CommitPreparedBatch(ctx, pb))
	require.Len(t, pb.sentinelUpdates, 2, "only the surviving ledger's latest volume updates remain")
	for _, update := range pb.sentinelUpdates {
		require.Equal(t, "live", update.Key.LedgerName)
	}
	for _, ledger := range []string{"first", "second", "live"} {
		pair, err := attrs.Volume.Get(store, domain.NewVolumeKey(ledger, "treasury", "EUR", "").Bytes())
		require.NoError(t, err)
		if ledger == "live" {
			require.NotNil(t, pair)
			require.Equal(t, "250", pair.GetInput().ToBigInt().String())
		} else {
			require.Nil(t, pair)
		}
	}
}

// Corrupt the pending write for a surviving ledger after preparation. The real
// post-commit sentinel must still detect missing and changed rows when another
// ledger is intentionally deleted in that same batch.
func TestDeleteLedgerSentinelDetectsSurvivorCorruption(t *testing.T) {
	t.Parallel()
	for _, mode := range []string{"missing", "changed"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			fsm, store, attrs := newTestMachine(t)
			fsm.sentinel = dal.NewSentinelFactory(store, true)
			fsm.sentinelMode = true
			ctx := context.Background()
			_, err := fsm.ApplyEntries(ctx, store, makeEntry(t, 1, makeProposal(1,
				createLedgerOrder("deleted"), createLedgerOrder("live"))))
			require.NoError(t, err)
			pb, err := fsm.PrepareEntries(ctx, store,
				makeEntry(t, 2, makeProposal(2,
					createTransactionOrder("deleted", true, newPosting("world", "treasury", "EUR", 100)),
					createTransactionOrder("live", true, newPosting("world", "treasury", "EUR", 200)))),
				makeEntry(t, 3, makeProposal(3, deleteLedgerOrder("deleted"))))
			require.NoError(t, err)
			for _, result := range pb.Result.Results {
				require.NoError(t, result.Error)
			}
			key := domain.NewVolumeKey("live", "treasury", "EUR", "")
			if mode == "missing" {
				require.NoError(t, attrs.Volume.Delete(pb.batch, key.Bytes()))
			} else {
				_, err = attrs.Volume.Set(pb.batch, key.Bytes(), &raftcmdpb.VolumePair{
					Input: commonpb.NewUint256FromUint64(201), Output: commonpb.NewUint256FromUint64(0),
				})
				require.NoError(t, err)
			}
			err = fsm.CommitPreparedBatch(ctx, pb)
			require.ErrorContains(t, err, "post-commit volume assertion failed")
			if mode == "missing" {
				require.ErrorContains(t, err, `volume missing from pebble after commit for "live"/treasury/EUR`)
			} else {
				var divergence *ErrVolumeCachePebbleDivergence
				require.ErrorAs(t, err, &divergence)
				require.Equal(t, key, divergence.Key)
				require.Equal(t, "200", divergence.CacheInput)
				require.Equal(t, "201", divergence.PebbleInput)
			}
		})
	}
}

// A deletion rolled back with its proposal must not remove earlier expectations
// or change the existing prohibition on recreating a deleted ledger name.
func TestDeleteLedgerSentinelRejectedProposal(t *testing.T) {
	t.Parallel()
	fsm, store, _ := newTestMachine(t)
	fsm.sentinel = dal.NewSentinelFactory(store, true)
	fsm.sentinelMode = true
	ctx := context.Background()
	_, err := fsm.ApplyEntries(ctx, store, makeEntry(t, 1, makeProposal(1, createLedgerOrder("live"))))
	require.NoError(t, err)
	pb, err := fsm.PrepareEntries(ctx, store,
		makeEntry(t, 2, makeProposal(2, createTransactionOrder("live", true, newPosting("world", "treasury", "EUR", 100)))),
		makeEntry(t, 3, makeProposal(3, deleteLedgerOrder("live"), createLedgerOrder("live"))))
	require.NoError(t, err)
	require.Len(t, pb.Result.Results, 2)
	require.NoError(t, pb.Result.Results[0].Error)
	var deleted *domain.ErrLedgerDeleted
	require.ErrorAs(t, pb.Result.Results[1].Error, &deleted)
	require.NoError(t, fsm.CommitPreparedBatch(ctx, pb))
	require.Len(t, pb.sentinelUpdates, 2, "a rejected deletion must not discard prior expected volumes")
	info, _, err := fsm.Registry.Ledgers.GetKey(domain.LedgerKey{Name: "live"})
	require.NoError(t, err)
	require.Nil(t, info.GetDeletedAt())
}
