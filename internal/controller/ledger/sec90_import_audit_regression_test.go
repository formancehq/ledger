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
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/features"
)

// sec90ImportedLog builds a synthetic NEW_TRANSACTION log as an external export
// would carry it: the persisted transaction embeds postCommitVolumes /
// postCommitEffectiveVolumes values supplied by the export.
func sec90ImportedLog(id uint64, postCommit, postCommitEffective ledger.PostCommitVolumes) ledger.Log {
	tx := ledger.NewTransaction().
		WithID(id).
		WithPostings(ledger.NewPosting("world", "alice", "USD/2", big.NewInt(100))).
		WithTimestamp(time.New(stdtime.Date(2026, 1, 1, 0, 0, 0, 0, stdtime.UTC))).
		WithPostCommitVolumes(postCommit).
		WithPostCommitEffectiveVolumes(postCommitEffective)
	return ledger.NewLog(ledger.CreatedTransaction{
		Transaction: tx,
	}).WithID(id)
}

// sec90CanonicalVolumes is the canonical recomputation of post-commit volumes
// for the synthetic posting world -> alice of 100 USD/2 on an empty ledger.
func sec90CanonicalVolumes() ledger.PostCommitVolumes {
	return ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(0, 100)},
		"alice": {"USD/2": ledger.NewVolumesInt64(100, 0)},
	}
}

// sec90SetupImportMock wires a MockStore the way DefaultController.Import uses
// it: empty ledger, per-log TX on the same store, and a captured InsertLog.
func sec90SetupImportMock(t *testing.T, captured *[]*ledger.Log) (*DefaultController, *MockStore) {
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

	// Capture the exact payload handed to InsertLog (what lands in logs.data).
	store.EXPECT().InsertLog(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, log *ledger.Log) error {
			*captured = append(*captured, log)
			return nil
		}).AnyTimes()

	// Emulate the real store contract: CommitTransaction recomputes
	// PostCommitVolumes from the postings (see storage/ledger/transactions.go),
	// overwriting whatever the imported payload supplied. Volumes are cumulative
	// account volumes: after the revert move alice->world of 100, alice has
	// input=100 (from the created tx) and output=100 (from the revert), and world
	// has output=100 then input=100.
	store.EXPECT().CommitTransaction(gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, tx *ledger.Transaction) error {
			if *tx.ID == 1 {
				tx.PostCommitVolumes = sec90CanonicalVolumes()
			} else {
				tx.PostCommitVolumes = ledger.PostCommitVolumes{
					"world": {"USD/2": ledger.NewVolumesInt64(100, 100)},
					"alice": {"USD/2": ledger.NewVolumesInt64(100, 100)},
				}
			}
			// Mirror the real store with the effective-volumes feature SYNC:
			// PCeV is recomputed in place from the moves, overwriting the supplied value.
			tx.PostCommitEffectiveVolumes = tx.PostCommitVolumes.Copy()
			return nil
		}).AnyTimes()

	store.EXPECT().UpsertAccounts(gomock.Any(), gomock.Any()).Return(nil).AnyTimes()
	store.EXPECT().RevertTransaction(gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(_ context.Context, id uint64, at time.Time) (*ledger.Transaction, bool, error) {
			tx := sec90ImportedLog(id, sec90CanonicalVolumes(), nil).Data.(ledger.CreatedTransaction).Transaction
			tx.RevertedAt = pointer.For(at)
			return &tx, true, nil
		}).AnyTimes()

	l := NewDefaultController(ledger.Ledger{
		Configuration: ledger.Configuration{
			Features: features.DefaultFeatures, // HASH_LOGS=SYNC, MOVES_HISTORY=ON, PVCe=SYNC
		},
	}, store, NewMockNumscriptParser(ctrl), NewMockNumscriptParser(ctrl), NewMockNumscriptParser(ctrl))
	return l, store
}

func sec90Import(t *testing.T, ctrl *DefaultController, logs ...ledger.Log) error {
	t.Helper()
	stream := make(chan ledger.Log, len(logs))
	for _, l := range logs {
		stream <- l
	}
	close(stream)
	return ctrl.Import(context.Background(), stream)
}

// SEC-90 regression: an imported NEW_TRANSACTION log whose payload carries
// stale/repaired postCommitVolumes must not be persisted verbatim into
// logs.data. The canonical, recomputed volumes (as produced by
// Store.CommitTransaction before InsertLog) are the audit source of truth.
func TestSec90ImportCreatedTransactionPersistsCanonicalVolumes(t *testing.T) {
	t.Parallel()

	var captured []*ledger.Log
	ctrl, _ := sec90SetupImportMock(t, &captured)

	stale := ledger.PostCommitVolumes{
		"world": {"USD/2": ledger.NewVolumesInt64(0, 999999)}, // repaired/stale export value
		"alice": {"USD/2": ledger.NewVolumesInt64(999999, 0)},
	}
	err := sec90Import(t, ctrl, sec90ImportedLog(1, stale, stale))
	require.NoError(t, err)
	require.Len(t, captured, 1)

	persisted := captured[0].Data.(ledger.CreatedTransaction).Transaction.PostCommitVolumes
	require.Equal(t, sec90CanonicalVolumes(), persisted,
		"logs.data must carry the recomputed postCommitVolumes, not the supplied ones")

	// The serialized payload (what InsertLog marshals into the jsonb column)
	// must not contain the stale value either.
	raw, err := json.Marshal(captured[0].Data)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "999999")
}

// Positive case: a historical/valid export whose supplied volumes already match
// the canonical recomputation must import cleanly (no false positive).
func TestSec90ImportCreatedTransactionValidVolumesAccepted(t *testing.T) {
	t.Parallel()

	var captured []*ledger.Log
	ctrl, _ := sec90SetupImportMock(t, &captured)

	err := sec90Import(t, ctrl, sec90ImportedLog(1, sec90CanonicalVolumes(), sec90CanonicalVolumes()))
	require.NoError(t, err)
	require.Len(t, captured, 1)
}

// Revert coverage: importing a REVERTED_TRANSACTION log after the created one;
// the revert payload embeds two transactions whose volumes are also persisted
// verbatim and must agree with the recomputed values.
func TestSec90ImportRevertedTransactionPersistsCanonicalVolumes(t *testing.T) {
	t.Parallel()

	var captured []*ledger.Log
	ctrl, _ := sec90SetupImportMock(t, &captured)

	created := sec90ImportedLog(1, sec90CanonicalVolumes(), sec90CanonicalVolumes())
	revertedAt := time.New(stdtime.Date(2026, 1, 2, 0, 0, 0, 0, stdtime.UTC))
	revertTx := ledger.NewTransaction().
		WithID(2).
		WithPostings(ledger.NewPosting("alice", "world", "USD/2", big.NewInt(100))).
		WithTimestamp(revertedAt).
		WithPostCommitVolumes(ledger.PostCommitVolumes{
			"world": {"USD/2": ledger.NewVolumesInt64(0, 0)},
			"alice": {"USD/2": ledger.NewVolumesInt64(0, 0)},
		})
	reverted := created.Data.(ledger.CreatedTransaction).Transaction.WithRevertedAt(revertedAt)
	revertLog := ledger.NewLog(ledger.RevertedTransaction{
		RevertedTransaction: reverted,
		RevertTransaction:   revertTx,
	}).WithID(2)

	err := sec90Import(t, ctrl, created, revertLog)
	require.NoError(t, err)
	require.Len(t, captured, 2)

	persistedRevert := captured[1].Data.(ledger.RevertedTransaction)
	// The revert transaction's canonical postCommitVolumes are cumulative: after
	// the full reversal both accounts carry input=100, output=100. The supplied
	// 0/0 values must not be persisted verbatim.
	require.Equal(t, ledger.NewVolumesInt64(100, 100), persistedRevert.RevertTransaction.PostCommitVolumes["alice"]["USD/2"])
}
