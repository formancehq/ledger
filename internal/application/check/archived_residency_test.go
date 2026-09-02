package check

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func newResidencyTestStore(t *testing.T) *dal.Store {
	t.Helper()

	store, err := dal.NewStore(t.TempDir(), logging.Testing(), noop.NewMeterProvider().Meter("test"), dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close() })

	return store
}

func writeColdKey(t *testing.T, store *dal.Store, sub byte, seq uint64, suffix ...uint32) {
	t.Helper()

	kb := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, sub).PutUint64(seq)
	for _, s := range suffix {
		kb = kb.PutUint32(s)
	}

	batch := store.OpenWriteSession()
	require.NoError(t, batch.Set(kb.Build(), []byte("x"), nil))
	require.NoError(t, batch.Commit())
}

func collectResidencyFindings(t *testing.T, store *dal.Store, chapters []*commonpb.Chapter) []*servicepb.CheckStoreError {
	t.Helper()

	handle, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	var got []*servicepb.CheckStoreError

	require.NoError(t, verifyArchivedChapterResidency(handle, chapters, func(event *servicepb.CheckStoreEvent) {
		e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error)
		require.True(t, ok, "the residency pass emits only error events")
		require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNPURGED_ARCHIVED_DATA, e.Error.GetErrorType())
		got = append(got, e.Error)
	}))

	return got
}

// An ARCHIVED chapter's purge ranges must be empty in hot storage; a resident
// key under any of the four cold sub-prefixes is reported per (chapter,
// sub-prefix) with a count. This is the checker side of the
// ConfirmArchiveChapter purge: it reports a lost purge, and any path that
// re-ingests archived history without purging it.
func TestVerifyArchivedChapterResidency_ReportsResidentKeys(t *testing.T) {
	t.Parallel()

	store := newResidencyTestStore(t)

	// Logs [10, 20], audit sequences [5, 8]. One resident key per sub-prefix
	// inside the ranges; audit items get two to pin the count.
	writeColdKey(t, store, dal.SubColdLog, 15)
	writeColdKey(t, store, dal.SubColdAudit, 6)
	writeColdKey(t, store, dal.SubColdAuditItem, 6, 0)
	writeColdKey(t, store, dal.SubColdAuditItem, 8, 2)
	writeColdKey(t, store, dal.SubColdAppliedProposal, 7)

	findings := collectResidencyFindings(t, store, []*commonpb.Chapter{{
		Id:                 3,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      10,
		CloseSequence:      20,
		StartAuditSequence: 5,
		CloseAuditSequence: 8,
	}})

	require.Len(t, findings, 4, "one finding per resident sub-prefix")
	require.Contains(t, findings[0].GetMessage(), "1 unpurged log key(s) in hot storage over [10, 20], first at sequence 15")
	require.Contains(t, findings[1].GetMessage(), "1 unpurged audit entry key(s) in hot storage over [5, 8], first at sequence 6")
	require.Contains(t, findings[2].GetMessage(), "2 unpurged audit item key(s) in hot storage over [5, 8], first at sequence 6")
	require.Contains(t, findings[3].GetMessage(), "1 unpurged applied proposal key(s) in hot storage over [5, 8], first at sequence 7")

	for _, f := range findings {
		require.Contains(t, f.GetMessage(), "archived chapter 3")
		require.Equal(t, uint64(20), f.GetLogSequence(), "events anchor on the chapter's close sequence")
	}
}

// Keys outside an ARCHIVED chapter's ranges, and keys inside the ranges of a
// chapter in any other status, are legitimate hot data the purge never covers.
func TestVerifyArchivedChapterResidency_IgnoresOutOfScopeKeys(t *testing.T) {
	t.Parallel()

	store := newResidencyTestStore(t)

	// Adjacent to the archived chapter's ranges (logs [10, 20], audit [5, 8]).
	writeColdKey(t, store, dal.SubColdLog, 9)
	writeColdKey(t, store, dal.SubColdLog, 21)
	writeColdKey(t, store, dal.SubColdAudit, 9)
	writeColdKey(t, store, dal.SubColdAuditItem, 4, 0)
	writeColdKey(t, store, dal.SubColdAuditItem, 9, 0)
	writeColdKey(t, store, dal.SubColdAppliedProposal, 4)

	// Inside the open chapter's ranges: live data, not residue.
	writeColdKey(t, store, dal.SubColdLog, 30)
	writeColdKey(t, store, dal.SubColdAudit, 12)
	writeColdKey(t, store, dal.SubColdAuditItem, 12, 0)

	findings := collectResidencyFindings(t, store, []*commonpb.Chapter{
		{
			Id:                 1,
			Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
			StartSequence:      10,
			CloseSequence:      20,
			StartAuditSequence: 5,
			CloseAuditSequence: 8,
		},
		{
			Id:                 2,
			Status:             commonpb.ChapterStatus_CHAPTER_OPEN,
			StartSequence:      21,
			CloseSequence:      40,
			StartAuditSequence: 9,
			CloseAuditSequence: 15,
		},
	})

	require.Empty(t, findings)
}

// The pass runs before Check's zero-log fast path: a store with no logs can
// still hold ARCHIVED chapter registry rows with residue in their purge
// ranges, and returning from the fast path without scanning would report
// that store clean.
func TestVerifyArchivedChapterResidency_RunsOnZeroLogStore(t *testing.T) {
	t.Parallel()

	store := newResidencyTestStore(t)

	batch := store.OpenWriteSession()
	chapterKey := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneGlobal, dal.SubGlobChapters).PutUint64(1).Build()
	require.NoError(t, batch.SetProto(chapterKey, &commonpb.Chapter{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      10,
		CloseSequence:      20,
		StartAuditSequence: 5,
		CloseAuditSequence: 8,
	}))
	require.NoError(t, batch.Commit())

	writeColdKey(t, store, dal.SubColdAuditItem, 6, 0)

	checker := NewChecker(store, attributes.New(), "test-cluster", nil, nil, nil, logging.Testing())

	var got []*servicepb.CheckStoreError

	require.NoError(t, checker.Check(context.Background(), func(event *servicepb.CheckStoreEvent) {
		if e, ok := event.GetType().(*servicepb.CheckStoreEvent_Error); ok &&
			e.Error.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_UNPURGED_ARCHIVED_DATA {
			got = append(got, e.Error)
		}
	}))

	require.Len(t, got, 1)
	require.Contains(t, got[0].GetMessage(), "1 unpurged audit item key(s)")
}

// A chapter that closed without audit entries carries close < start on the
// audit-keyed ranges; the scan must skip them the way the purge does instead
// of iterating an inverted range.
func TestVerifyArchivedChapterResidency_SkipsEmptyAuditRange(t *testing.T) {
	t.Parallel()

	store := newResidencyTestStore(t)
	writeColdKey(t, store, dal.SubColdAuditItem, 5, 0)

	findings := collectResidencyFindings(t, store, []*commonpb.Chapter{{
		Id:                 1,
		Status:             commonpb.ChapterStatus_CHAPTER_ARCHIVED,
		StartSequence:      10,
		CloseSequence:      20,
		StartAuditSequence: 6,
		CloseAuditSequence: 5,
	}})

	require.Empty(t, findings)
}
