package query_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func seedPreparedQuery(t *testing.T, s *dal.Store, attrs *attributes.Attributes, ledger, name string, target commonpb.QueryTarget, filter *commonpb.QueryFilter) {
	t.Helper()

	batch := s.OpenWriteSession()
	_, err := attrs.PreparedQuery.Set(batch, domain.PreparedQueryKey{LedgerName: ledger, Name: name}.Bytes(), &commonpb.PreparedQuery{
		Name:   name,
		Target: target,
		Filter: filter,
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

type mutatingQueryHandleStore struct {
	store      *dal.Store
	beforeOpen func()
}

type failingQueryHandleStore struct{ err error }

func (s failingQueryHandleStore) NewReadHandle() (*dal.ReadHandle, error) {
	return nil, s.err
}

func TestExecutePropagatesMainSnapshotOpenFailure(t *testing.T) {
	t.Parallel()

	rs := newTestReadStore(t)
	attrs := attributes.New()
	wantErr := errors.New("open main snapshot")

	_, err := query.Execute(
		t.Context(), rs, failingQueryHandleStore{err: wantErr},
		attrs.Volume, attrs.PreparedQuery, attrs.Index,
		&servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q"}, nil, nil,
	)
	require.ErrorIs(t, err, wantErr)
}

func (s *mutatingQueryHandleStore) NewReadHandle() (*dal.ReadHandle, error) {
	s.beforeOpen()

	return s.store.NewReadHandle()
}

func TestExecute_ReadsPreparedQueryFromMainSnapshot(t *testing.T) {
	t.Parallel()

	store := newTestStore(t)
	registerLedger(t, store, "l")

	rs := newTestReadStore(t)
	attrs := attributes.New()
	seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)

	opener := &mutatingQueryHandleStore{
		store: store,
		beforeOpen: func() {
			batch := store.OpenWriteSession()
			require.NoError(t, attrs.PreparedQuery.Delete(batch, domain.PreparedQueryKey{LedgerName: "l", Name: "q"}.Bytes()))
			require.NoError(t, batch.Commit())
		},
	}

	_, err := query.Execute(
		t.Context(), rs, opener, attrs.Volume, attrs.PreparedQuery, attrs.Index,
		&servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q"}, nil, nil,
	)
	var notFound *domain.ErrPreparedQueryNotFound
	require.ErrorAs(t, err, &notFound,
		"the prepared query must be read after opening the main snapshot")
}

// A prepared query that reads no index leaf must not be gated on the fold.
// Its universe and its enrichment both come from the main-store handle, so a
// builder that is lagging — or stopped — says nothing about whether the
// answer is available, and waiting would fail a read on data it never touches.
func TestExecute_UnfilteredQueryDoesNotWaitForTheFold(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		target  commonpb.QueryTarget
		filter  *commonpb.QueryFilter
		blocked bool
	}{
		{name: "unfiltered accounts", target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, blocked: false},
		{name: "unfiltered transactions", target: commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, blocked: false},
		{name: "unfiltered logs", target: commonpb.QueryTarget_QUERY_TARGET_LOGS, blocked: true},
		{
			name:   "filtered accounts",
			target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			filter: &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
				Field: &commonpb.FieldCondition{
					Field:     &commonpb.FieldRef{Metadata: "tier"},
					Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
				},
			}},
			blocked: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			store := newTestStore(t)
			registerLedger(t, store, "l")
			appendLogs(t, store, 3, createTestLogsForLedger("l", 1)...)

			rs := newTestReadStore(t)
			setReadStoreProgress(t, rs, 1) // permanently behind the main store

			attrs := attributes.New()
			seedPreparedQuery(t, store, attrs, "l", "q", tc.target, tc.filter)
			req := &servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q"}

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			_, err := query.Execute(ctx, rs, store, attrs.Volume, attrs.PreparedQuery, attrs.Index, req, nil, nil)

			if tc.blocked {
				require.ErrorIs(t, err, context.DeadlineExceeded, "a read that consults the index is owed alignment")

				return
			}

			require.NotErrorIs(t, err, context.DeadlineExceeded, "must not wait on a fold it does not read")
		})
	}
}
