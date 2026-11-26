package query_test

import (
	"errors"
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// softDeleteLedger commits the main-store effect of DeleteLedger.
func softDeleteLedger(t *testing.T, store *dal.Store, name string) {
	t.Helper()

	batch := store.OpenWriteSession()
	require.NoError(t, state.SaveLedger(batch, name, &commonpb.LedgerInfo{Name: name, DeletedAt: &commonpb.Timestamp{}}))
	require.NoError(t, batch.Commit())
}

// foldLedgerDeletion commits the projection effect of DeleteLedger: every
// ledger-scoped row is gone, the certificate is unchanged.
func foldLedgerDeletion(t *testing.T, rs *readstore.Store, name string) {
	t.Helper()

	batch := rs.NewBatch()
	require.NoError(t, readstore.DeleteLedgerIndexes(batch, name))
	require.NoError(t, batch.Commit())
}

// A ledger deleted after the main handle pinned it live must turn the aligned
// read into a NotFound rejection, whether or not the projection has folded the
// wipe yet (EN-1991). The pinned main snapshot alone cannot tell: it still
// holds the ledger live.
func TestAlignedIndexSnapshotRejectsLedgerDeletedAfterPin(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		folded bool
	}{
		{name: "deletion folded into the projection", folded: true},
		{name: "deletion applied, projection not yet folded", folded: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestStore(t)
			registerLedger(t, store, "l")
			appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)
			rs := newTestReadStore(t)
			setReadStoreProgress(t, rs, 3)

			handle, err := store.NewReadHandle()
			require.NoError(t, err)
			defer func() { _ = handle.Close() }()

			pinned, err := query.GetLedgerByName(t.Context(), handle, "l")
			require.NoError(t, err)
			require.Nil(t, pinned.GetDeletedAt())

			softDeleteLedger(t, store, "l")
			if tc.folded {
				foldLedgerDeletion(t, rs, "l")
			}

			_, _, _, err = query.AlignedIndexSnapshot(t.Context(), rs, handle, "l", func() {})

			var notFound *domain.ErrLedgerNotFound
			require.ErrorAs(t, err, &notFound)
			require.Equal(t, "l", notFound.Name)
			// The rejection released its lease: the GC watermark is unpinned.
			require.Equal(t, uint64(math.MaxUint64), rs.Leases().BeginGC(math.MaxUint64))
		})
	}
}

// A vanished LedgerInfo row is unreachable by contract: CreateLedger writes it
// and DeleteLedger only stamps DeletedAt. The gate must say so rather than
// report the ordinary deletion it is not.
func TestAlignedIndexSnapshotFailsLoudlyWhenPinnedLedgerRowVanishes(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)
	rs := newTestReadStore(t)
	setReadStoreProgress(t, rs, 3)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	kb := dal.NewKeyBuilder()
	kb.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobLedgerInfo).PutLedgerName("l")
	batch := store.OpenWriteSession()
	require.NoError(t, batch.DeleteKey(kb.Build()))
	require.NoError(t, batch.Commit())

	_, _, _, err = query.AlignedIndexSnapshot(t.Context(), rs, handle, "l", func() {})

	require.ErrorContains(t, err, "invariant")

	var notFound *domain.ErrLedgerNotFound
	require.False(t, errors.As(err, &notFound), "an impossible state must not read as a deletion")
}

func TestAlignedIndexSnapshotServesLiveLedger(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")
	appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)
	rs := newTestReadStore(t)
	setReadStoreProgress(t, rs, 3)

	handle, err := store.NewReadHandle()
	require.NoError(t, err)
	defer func() { _ = handle.Close() }()

	pinnedSeq, err := query.ReadLastSequence(handle)
	require.NoError(t, err)

	snap, mainSeq, release, err := query.AlignedIndexSnapshot(t.Context(), rs, handle, "l", func() {})
	require.NoError(t, err)
	defer release()
	defer func() { _ = snap.Close() }()

	require.Equal(t, pinnedSeq, mainSeq)
}

// The prepared LOGS path pins the ledger and the definition on the main
// handle, then aligns. A deletion committed and folded in between must
// surface as ErrLedgerNotFound, never as a successful empty cursor.
func TestExecute_LogsQueryRejectsLedgerDeletedAfterPin(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		deleted bool
	}{
		{name: "ledger deleted and wiped after the pin", deleted: true},
		{name: "ledger live", deleted: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestStore(t)
			registerLedger(t, store, "l")
			appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)
			rs := newTestReadStore(t)
			setReadStoreProgress(t, rs, 3)
			attrs := attributes.New()
			seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_LOGS, &commonpb.QueryFilter{
				Filter: &commonpb.QueryFilter_Ledger{Ledger: &commonpb.LedgerCondition{
					Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: "l"}},
				}},
			})

			opener := &mutatingQueryHandleStore{Store: store}
			if tc.deleted {
				opener.afterOpen = func() {
					softDeleteLedger(t, store, "l")
					foldLedgerDeletion(t, rs, "l")
				}
			}

			resp, err := query.Execute(
				t.Context(), rs, opener, attrs.Volume, attrs.PreparedQuery, attrs.Index,
				&servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q", Mode: commonpb.QueryMode_QUERY_MODE_LIST},
				nil, nil,
			)

			if !tc.deleted {
				require.NoError(t, err)
				require.NotNil(t, resp.GetCursor())

				return
			}

			var notFound *domain.ErrLedgerNotFound
			require.ErrorAs(t, err, &notFound)
			require.Nil(t, resp)
		})
	}
}
