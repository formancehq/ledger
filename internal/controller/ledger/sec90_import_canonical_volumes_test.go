package ledger

import (
	"context"
	"encoding/json"
	"math/big"
	"testing"
	stdtime "time"

	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/features"
)

// sec90EffectiveVolumes mirrors the real store contract
// (Store.CommitTransaction with MOVES_HISTORY=ON and
// MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES=SYNC): for a single posting on an
// empty ledger, the effective volumes equal the post-commit volumes.
func sec90EffectiveVolumes(pcv ledger.PostCommitVolumes) ledger.PostCommitVolumes {
	return pcv.Copy()
}

// sec90SetupFidfulImportMock wires a MockStore that mirrors the real storage
// contract on the import path:
//   - CommitTransaction recomputes PostCommitVolumes as *cumulative* account
//     volumes (UpdateVolumes upserts and returns the accumulated row), and with
//     the effective-volumes feature SYNC it also recomputes
//     PostCommitEffectiveVolumes from the inserted moves.
//   - RevertTransaction returns the original transaction row as reverted, with
//     its persisted postCommitVolumes.
//
// A running cumulative volume state is kept across logs so a revert log sees
// meaningful, non-tautological volumes.
func sec90SetupFidfulImportMock(t *testing.T, fs features.FeatureSet, captured *[]*ledger.Log) (*DefaultController, *MockStore) {
	t.Helper()
	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)

	logsResource := NewMockPaginatedResource[ledger.Log, any](ctrl)
	store.EXPECT().Logs().Return(logsResource).AnyTimes()
	logsResource.EXPECT().
		Paginate(gomock.Any(), gomock.Any()).
		Return(&paginate.Cursor[ledger.Log]{Data: []ledger.Log{}}, nil).
		AnyTimes()

	store.EXPECT().BeginTX(gomock.Any(), gomock.Any()).Return(store, &bun.Tx{}, nil).AnyTimes()
	store.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	store.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()

	store.EXPECT().InsertLog(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, log *ledger.Log) error {
			*captured = append(*captured, log)
			return nil
		}).AnyTimes()

	// Cumulative per account/asset volumes, like accounts_volumes rows.
	cumulative := ledger.PostCommitVolumes{}

	store.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, tx *ledger.Transaction) error {
			for _, update := range tx.VolumeUpdates() {
				if _, ok := cumulative[update.Account]; !ok {
					cumulative[update.Account] = ledger.VolumesByAssets{}
				}
				if _, ok := cumulative[update.Account][update.Asset]; !ok {
					cumulative[update.Account][update.Asset] = ledger.NewEmptyVolumes()
				}
				cumulative.AddInput(update.Account, update.Asset, update.Input)
				cumulative.AddOutput(update.Account, update.Asset, update.Output)
			}
			tx.PostCommitVolumes = touchedEntriesForTest(tx.Postings, cumulative)
			if fs[features.FeatureMovesHistory] == "ON" && fs[features.FeatureMovesHistoryPostCommitEffectiveVolumes] == "SYNC" {
				tx.PostCommitEffectiveVolumes = sec90EffectiveVolumes(tx.PostCommitVolumes)
			}
			return nil
		}).AnyTimes()

	store.EXPECT().UpsertAccounts(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// RevertTransaction returns the stored original transaction (with its
	// persisted PCV) marked reverted at the requested date.
	store.EXPECT().RevertTransaction(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, id uint64, at time.Time) (*ledger.Transaction, bool, error) {
			tx := ledger.NewTransaction().
				WithID(id).
				WithPostings(ledger.NewPosting("world", "alice", "USD/2", big.NewInt(100))).
				WithTimestamp(time.New(stdtime.Date(2026, 1, 1, 0, 0, 0, 0, stdtime.UTC))).
				WithPostCommitVolumes(touchedEntriesForTest(
					ledger.Postings{ledger.NewPosting("world", "alice", "USD/2", big.NewInt(100))},
					cumulative,
				)).
				WithRevertedAt(at)
			return &tx, true, nil
		}).AnyTimes()

	l := NewDefaultController(ledger.Ledger{
		Configuration: ledger.Configuration{
			Features: fs,
		},
	}, store, NewMockNumscriptParser(ctrl), NewMockNumscriptParser(ctrl), NewMockNumscriptParser(ctrl))
	return l, store
}

func touchedEntriesForTest(postings ledger.Postings, volumes ledger.PostCommitVolumes) ledger.PostCommitVolumes {
	tx := ledger.Transaction{TransactionData: ledger.TransactionData{Postings: postings}}
	ret := ledger.PostCommitVolumes{}
	for _, u := range tx.VolumeUpdates() {
		if byAssets, ok := volumes[u.Account]; ok {
			if v, ok := byAssets[u.Asset]; ok {
				if _, ok := ret[u.Account]; !ok {
					ret[u.Account] = ledger.VolumesByAssets{}
				}
				ret[u.Account][u.Asset] = v
			}
		}
	}
	return ret
}

func sec90NewImportController(t *testing.T, fs features.FeatureSet, captured *[]*ledger.Log) *DefaultController {
	t.Helper()
	ctrl, _ := sec90SetupFidfulImportMock(t, fs, captured)
	return ctrl
}

func sec90ImportLogs(t *testing.T, ctrl *DefaultController, logs ...ledger.Log) error {
	t.Helper()
	stream := make(chan ledger.Log, len(logs))
	for _, l := range logs {
		stream <- l
	}
	close(stream)
	return ctrl.Import(t.Context(), stream)
}

func sec90CreatedLog(id uint64, pcv, pceV ledger.PostCommitVolumes) ledger.Log {
	tx := ledger.NewTransaction().
		WithID(id).
		WithPostings(ledger.NewPosting("world", "alice", "USD/2", big.NewInt(100))).
		WithTimestamp(time.New(stdtime.Date(2026, 1, 1, 0, 0, 0, 0, stdtime.UTC))).
		WithPostCommitVolumes(pcv).
		WithPostCommitEffectiveVolumes(pceV)
	return ledger.NewLog(ledger.CreatedTransaction{Transaction: tx}).WithID(id)
}

func sec90StaleVolumes() ledger.PostCommitVolumes {
	return ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(0, 999999)},
		"alice": {
			"USD/2": ledger.NewVolumesInt64(999999, 0),
			"EUR":   ledger.NewVolumesInt64(42, 0), // untouched asset on a touched account
		},
		// Entry present only in the supplied volumes: no posting touches bob/EUR.
		"bob": {"EUR": ledger.NewVolumesInt64(42, 0)},
	}
}

// The persisted serialized payload (what InsertLog marshals into logs.data)
// must carry canonical PCV and PCeV for a created transaction, and must not
// contain any supplied stale or extra account/asset entry.
func TestSec90ImportCreatedLogPersistsSerializedCanonicalPCVAndPCeV(t *testing.T) {
	t.Parallel()

	var captured []*ledger.Log
	ctrl := sec90NewImportController(t, features.DefaultFeatures, &captured)

	require.NoError(t, sec90ImportLogs(t, ctrl, sec90CreatedLog(1, sec90StaleVolumes(), sec90StaleVolumes())))
	require.Len(t, captured, 1)

	tx := captured[0].Data.(ledger.CreatedTransaction).Transaction
	canonical := ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(0, 100)},
		"alice": {"USD/2": ledger.NewVolumesInt64(100, 0)},
	}
	require.Equal(t, canonical, tx.PostCommitVolumes)
	require.Equal(t, canonical, tx.PostCommitEffectiveVolumes)

	raw, err := json.Marshal(captured[0].Data)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "999999")
	require.NotContains(t, string(raw), `"bob"`)
	require.NotContains(t, string(raw), `"EUR"`)
}

// Feature disabled (MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES=DISABLED):
// supplied effective volumes are scan-only and must never be persisted.
func TestSec90ImportCreatedLogPCeVDisabledDropsSuppliedEffectiveVolumes(t *testing.T) {
	t.Parallel()

	fs := features.DefaultFeatures.With(features.FeatureMovesHistoryPostCommitEffectiveVolumes, "DISABLED")
	var captured []*ledger.Log
	ctrl := sec90NewImportController(t, fs, &captured)

	require.NoError(t, sec90ImportLogs(t, ctrl, sec90CreatedLog(1, sec90StaleVolumes(), sec90StaleVolumes())))
	require.Len(t, captured, 1)

	tx := captured[0].Data.(ledger.CreatedTransaction).Transaction
	require.Equal(t, ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(0, 100)},
		"alice": {"USD/2": ledger.NewVolumesInt64(100, 0)},
	}, tx.PostCommitVolumes)
	require.Nil(t, tx.PostCommitEffectiveVolumes)

	raw, err := json.Marshal(captured[0].Data)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "postCommitEffectiveVolumes")
	require.NotContains(t, string(raw), "999999")
	require.NotContains(t, string(raw), `"bob"`)
}

// The store recomputes effective volumes only when BOTH features are enabled.
// MOVES_HISTORY=OFF with PCeV=SYNC is an accepted configuration, not a reason
// to reject a historical export or to retain its supplied effective volumes.
func TestSec90ImportEffectiveVolumeFeatureMatrix(t *testing.T) {
	t.Parallel()

	for _, moves := range []string{"ON", "OFF"} {
		for _, effective := range []string{"SYNC", "DISABLED"} {
			t.Run(moves+"/"+effective, func(t *testing.T) {
				t.Parallel()

				fs := features.DefaultFeatures.
					With(features.FeatureMovesHistory, moves).
					With(features.FeatureMovesHistoryPostCommitEffectiveVolumes, effective)
				var captured []*ledger.Log
				ctrl := sec90NewImportController(t, fs, &captured)
				created := sec90CreatedLog(1, sec90StaleVolumes(), sec90StaleVolumes())
				at := time.New(stdtime.Date(2026, 1, 2, 0, 0, 0, 0, stdtime.UTC))
				reversal := ledger.NewTransaction().WithID(2).
					WithPostings(ledger.NewPosting("alice", "world", "USD/2", big.NewInt(100))).
					WithTimestamp(at).
					WithPostCommitVolumes(sec90StaleVolumes()).
					WithPostCommitEffectiveVolumes(sec90StaleVolumes())
				reverted := ledger.NewLog(ledger.RevertedTransaction{
					RevertedTransaction: created.Data.(ledger.CreatedTransaction).Transaction.WithRevertedAt(at),
					RevertTransaction:   reversal,
				}).WithID(2)

				require.NoError(t, sec90ImportLogs(t, ctrl, created, reverted))
				require.Len(t, captured, 2)
				importedCreated := captured[0].Data.(ledger.CreatedTransaction).Transaction
				importedRevert := captured[1].Data.(ledger.RevertedTransaction)
				for _, tx := range []ledger.Transaction{importedCreated, importedRevert.RevertTransaction} {
					if moves == "ON" && effective == "SYNC" {
						require.Equal(t, tx.PostCommitVolumes, tx.PostCommitEffectiveVolumes)
					} else {
						require.Nil(t, tx.PostCommitEffectiveVolumes)
					}
				}
				require.Nil(t, importedRevert.RevertedTransaction.PostCommitEffectiveVolumes)
				for _, log := range captured {
					raw, err := json.Marshal(log.Data)
					require.NoError(t, err)
					require.NotContains(t, string(raw), "999999")
					require.NotContains(t, string(raw), `"bob"`)
					require.NotContains(t, string(raw), `"EUR"`)
				}
			})
		}
	}
}

// A valid export (supplied volumes already canonical) imports unchanged:
// no false positive, PCV and PCeV round-trip exactly.
func TestSec90ImportCreatedLogValidExportRoundTrips(t *testing.T) {
	t.Parallel()

	var captured []*ledger.Log
	ctrl := sec90NewImportController(t, features.DefaultFeatures, &captured)

	canonical := ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(0, 100)},
		"alice": {"USD/2": ledger.NewVolumesInt64(100, 0)},
	}
	require.NoError(t, sec90ImportLogs(t, ctrl, sec90CreatedLog(1, canonical, canonical)))
	require.Len(t, captured, 1)

	tx := captured[0].Data.(ledger.CreatedTransaction).Transaction
	require.Equal(t, canonical, tx.PostCommitVolumes)
	require.Equal(t, canonical, tx.PostCommitEffectiveVolumes)
}

// Historical export with missing volume fields (pre-migration-19 logs may lack
// postCommitVolumes): the import must still persist the canonical recomputed
// values, not null.
func TestSec90ImportCreatedLogHistoricalMissingVolumesGetCanonical(t *testing.T) {
	t.Parallel()

	var captured []*ledger.Log
	ctrl := sec90NewImportController(t, features.DefaultFeatures, &captured)

	require.NoError(t, sec90ImportLogs(t, ctrl, sec90CreatedLog(1, nil, nil)))
	require.Len(t, captured, 1)

	tx := captured[0].Data.(ledger.CreatedTransaction).Transaction
	require.Equal(t, ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(0, 100)},
		"alice": {"USD/2": ledger.NewVolumesInt64(100, 0)},
	}, tx.PostCommitVolumes)
	require.Equal(t, tx.PostCommitVolumes, tx.PostCommitEffectiveVolumes)
}

// Revert path: the persisted REVERTED_TRANSACTION log carries the canonical PCV
// of the revert transaction (cumulative volumes after the reversal move, per the
// real store contract), the original transaction re-read from the store with its
// own persisted PCV, and canonical PCeV for the revert transaction.
func TestSec90ImportRevertLogPersistsCanonicalVolumesBothTransactions(t *testing.T) {
	t.Parallel()

	var captured []*ledger.Log
	ctrl := sec90NewImportController(t, features.DefaultFeatures, &captured)

	created := sec90CreatedLog(1, sec90StaleVolumes(), sec90StaleVolumes())
	revertedAt := time.New(stdtime.Date(2026, 1, 2, 0, 0, 0, 0, stdtime.UTC))
	revertTx := ledger.NewTransaction().
		WithID(2).
		WithPostings(ledger.NewPosting("alice", "world", "USD/2", big.NewInt(100))).
		WithTimestamp(revertedAt).
		WithPostCommitVolumes(sec90StaleVolumes()). // stale supplied values
		WithPostCommitEffectiveVolumes(sec90StaleVolumes())
	revertLog := ledger.NewLog(ledger.RevertedTransaction{
		RevertedTransaction: created.Data.(ledger.CreatedTransaction).Transaction.WithRevertedAt(revertedAt),
		RevertTransaction:   revertTx,
	}).WithID(2)

	require.NoError(t, sec90ImportLogs(t, ctrl, created, revertLog))
	require.Len(t, captured, 2)

	payload := captured[1].Data.(ledger.RevertedTransaction)

	// Revert transaction: canonical cumulative volumes after world->alice 100 then
	// alice->world 100 => input=output=100 on both accounts.
	canonicalRevert := ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(100, 100)},
		"alice": {"USD/2": ledger.NewVolumesInt64(100, 100)},
	}
	require.Equal(t, canonicalRevert, payload.RevertTransaction.PostCommitVolumes)
	require.Equal(t, canonicalRevert, payload.RevertTransaction.PostCommitEffectiveVolumes)

	// Reverted original: re-read from the store, carrying its persisted PCV.
	require.Equal(t, ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(0, 100)},
		"alice": {"USD/2": ledger.NewVolumesInt64(100, 0)},
	}, payload.RevertedTransaction.PostCommitVolumes)
	require.True(t, payload.RevertedTransaction.IsReverted())

	raw, err := json.Marshal(captured[1].Data)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "999999")
	require.NotContains(t, string(raw), `"bob"`)
}

// Original-hash / memento contract: the log hash supplied by the export is kept
// byte-for-byte, and the memento still excludes volume fields. Import with
// HASH_LOGS=SYNC compares log.Hash before/after InsertLog and must not fail.
func TestSec90ImportKeepsHashAndMementoScopeUnchanged(t *testing.T) {
	t.Parallel()

	var captured []*ledger.Log
	ctrl := sec90NewImportController(t, features.DefaultFeatures, &captured)

	log := sec90CreatedLog(1, sec90StaleVolumes(), sec90StaleVolumes())
	log.Hash = []byte("0123456789abcdef0123456789abcdef")
	require.NoError(t, sec90ImportLogs(t, ctrl, log))
	require.Len(t, captured, 1)
	require.Equal(t, []byte("0123456789abcdef0123456789abcdef"), captured[0].Hash)

	memento, err := json.Marshal(captured[0].Data.(ledger.Memento).GetMemento())
	require.NoError(t, err)
	require.NotContains(t, string(memento), "postCommitVolumes")
	require.NotContains(t, string(memento), "postCommitEffectiveVolumes")
	require.NotContains(t, string(memento), "999999")
}

// Negative contract: a supplied hash that does not match the post-insert value
// is rejected when HASH_LOGS=SYNC (existing behavior, must keep holding).
func TestSec90ImportHashMismatchDetected(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := NewMockStore(ctrl)

	logsResource := NewMockPaginatedResource[ledger.Log, any](ctrl)
	store.EXPECT().Logs().Return(logsResource).AnyTimes()
	logsResource.EXPECT().
		Paginate(gomock.Any(), gomock.Any()).
		Return(&paginate.Cursor[ledger.Log]{Data: []ledger.Log{}}, nil).
		AnyTimes()
	store.EXPECT().BeginTX(gomock.Any(), gomock.Any()).Return(store, &bun.Tx{}, nil).AnyTimes()
	store.EXPECT().Rollback(gomock.Any()).Return(nil).AnyTimes()
	store.EXPECT().Commit(gomock.Any()).Return(nil).AnyTimes()
	store.EXPECT().InsertLog(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, log *ledger.Log) error {
			log.Hash = []byte("mutated-by-store")
			return nil
		}).AnyTimes()
	store.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, tx *ledger.Transaction) error {
			tx.PostCommitVolumes = touchedEntriesForTest(tx.Postings, ledger.PostCommitVolumes{
				"world": {"USD/2": ledger.NewVolumesInt64(0, 100)},
				"alice": {"USD/2": ledger.NewVolumesInt64(100, 0)},
			})
			tx.PostCommitEffectiveVolumes = tx.PostCommitVolumes.Copy()
			return nil
		}).AnyTimes()
	store.EXPECT().UpsertAccounts(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	l := NewDefaultController(ledger.Ledger{
		Configuration: ledger.Configuration{Features: features.DefaultFeatures},
	}, store, NewMockNumscriptParser(ctrl), NewMockNumscriptParser(ctrl), NewMockNumscriptParser(ctrl))

	log := sec90CreatedLog(1, nil, nil)
	log.Hash = []byte("original")
	err := sec90ImportLogs(t, l, log)
	require.Error(t, err)
	require.Contains(t, err.Error(), "invalid hash")
}

// Representative historical stale entry on the reverted original transaction:
// the supplied revertedTransaction.postCommitVolumes must be replaced by the
// store-returned row values.
func TestSec90ImportRevertOriginalTransactionVolumesComeFromStore(t *testing.T) {
	t.Parallel()

	var captured []*ledger.Log
	ctrl := sec90NewImportController(t, features.DefaultFeatures, &captured)

	created := sec90CreatedLog(1, sec90StaleVolumes(), sec90StaleVolumes())
	revertedAt := time.New(stdtime.Date(2026, 1, 2, 0, 0, 0, 0, stdtime.UTC))
	// Supplied reverted original carries stale volumes that must be discarded.
	revertedOriginal := created.Data.(ledger.CreatedTransaction).Transaction.
		WithRevertedAt(revertedAt).
		WithPostCommitVolumes(ledger.PostCommitVolumes{
			"world": {"USD/2": ledger.NewVolumesInt64(7, 7)},
			"alice": {"USD/2": ledger.NewVolumesInt64(7, 7)},
		})
	revertTx := ledger.NewTransaction().
		WithID(2).
		WithPostings(ledger.NewPosting("alice", "world", "USD/2", big.NewInt(100))).
		WithTimestamp(revertedAt)
	revertLog := ledger.NewLog(ledger.RevertedTransaction{
		RevertedTransaction: revertedOriginal,
		RevertTransaction:   revertTx,
	}).WithID(2)

	require.NoError(t, sec90ImportLogs(t, ctrl, created, revertLog))
	require.Len(t, captured, 2)

	payload := captured[1].Data.(ledger.RevertedTransaction)
	require.Equal(t, ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(0, 100)},
		"alice": {"USD/2": ledger.NewVolumesInt64(100, 0)},
	}, payload.RevertedTransaction.PostCommitVolumes)

	raw, err := json.Marshal(payload)
	require.NoError(t, err)
	require.NotContains(t, string(raw), `"input":7`)
}
