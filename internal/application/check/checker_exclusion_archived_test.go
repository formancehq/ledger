package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// writeAppliedProposal persists an AppliedProposal row at the given audit
// sequence, byte-for-byte as the FSM (and a restore's raw segment ingest)
// would store it.
func writeAppliedProposal(t *testing.T, store *dal.Store, entry *proposalpb.AppliedProposal) {
	t.Helper()

	batch := store.OpenWriteSession()
	key := dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).
		PutUint64(entry.GetSequence()).
		Build()
	require.NoError(t, batch.SetProto(key, entry))
	require.NoError(t, batch.Commit())
}

func transientProposal(seq uint64, ledger, account, asset string) *proposalpb.AppliedProposal {
	return &proposalpb.AppliedProposal{
		Sequence: seq,
		TransientVolumes: map[string]*proposalpb.TouchedVolumeList{
			ledger: {Volumes: []*commonpb.TouchedVolume{{Account: account, Asset: asset}}},
		},
	}
}

// TestCollectStoredTransientVolumes_SkipsArchivedAuditRange pins the scan's
// lower bound: proposals at or below the archived audit boundary are outside
// the replay's derivation window and must not enter the stored exclusion set.
func TestCollectStoredTransientVolumes_SkipsArchivedAuditRange(t *testing.T) {
	t.Parallel()

	store := createTestStore(t)
	writeAppliedProposal(t, store, transientProposal(1, "L", "t:below", "USD"))
	writeAppliedProposal(t, store, transientProposal(2, "L", "t:boundary", "USD"))
	writeAppliedProposal(t, store, transientProposal(3, "L", "t:above", "USD"))

	checker := NewChecker(store, nil, "test-cluster", nil, nil, nil, logging.Testing())

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	var got []string

	require.NoError(t, checker.collectStoredTransientVolumes(context.Background(), handle, 2,
		func(_, account, _, _ string) {
			got = append(got, account)
		}))
	require.Equal(t, []string{"t:above"}, got,
		"rows at or below the archived audit boundary must be skipped")

	got = nil
	require.NoError(t, checker.collectStoredTransientVolumes(context.Background(), handle, 0,
		func(_, account, _, _ string) {
			got = append(got, account)
		}))
	require.Equal(t, []string{"t:below", "t:boundary", "t:above"}, got,
		"without archived chapters the scan stays unbounded")
}

// TestCheck_RetainedTransientRecordsBelowArchiveBoundary is the model-test
// regression: a restore re-ingests the backfilled archived ranges — including
// AppliedProposal rows carrying TransientVolumes — and never re-purges them.
// The replay skips the matching logs (seq <= archiveEndSeq), so deriving
// nothing for them is correct; the stored side must skip the same window or a
// healthy restored store reports EXCLUSION_RECORD_MISMATCH for every
// transient account used before the archival.
func TestCheck_RetainedTransientRecordsBelowArchiveBoundary(t *testing.T) {
	t.Parallel()

	engine := newTestEngine(t)
	engine.processAndCommit(createLedgerOrder("test"))
	engine.processAndCommit(createTransactionOrder("test", false,
		newPosting("world", "alice", "USD", 100)))

	handle, err := engine.store.NewReadHandle()
	require.NoError(t, err)

	lastLog, err := query.ReadLastLog(handle)
	require.NoError(t, err)

	lastAudit, err := query.ReadLastAuditSequence(handle)
	require.NoError(t, err)

	// The boundary-time baseline snapshot, as chapter close writes it — without
	// it Check() skips entry-by-entry verification entirely under archiving.
	baselinePath, err := engine.store.BaselineSnapshotDir()
	require.NoError(t, err)
	require.NoError(t, attributes.CreateBaselineSnapshot(handle, baselinePath))
	require.NoError(t, handle.Close())

	// The transient record lives in the range about to be marked archived,
	// exactly as a backfilled restore retains it.
	writeAppliedProposal(t, engine.store, transientProposal(1, "test", "t:42", "USD"))

	// Post-boundary activity so the replay window is non-empty.
	engine.processAndCommit(createTransactionOrder("test", false,
		newPosting("world", "bob", "USD", 5)))

	batch := engine.store.OpenWriteSession()
	require.NoError(t, state.StoreChapter(batch, &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      1,
		CloseSequence:      lastLog.GetSequence(),
		StartAuditSequence: 1,
		CloseAuditSequence: lastAudit,
	}))
	require.NoError(t, batch.Commit())

	for _, e := range collectCheckErrors(t, engine.store, engine.attrs) {
		require.NotEqual(t,
			servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_EXCLUSION_RECORD_MISMATCH,
			e.GetErrorType(),
			"retained sub-boundary exclusion records must not be reported: %s", e.GetMessage())
	}
}
