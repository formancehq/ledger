package query_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/infra/state"
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

// mutatingQueryHandleStore commits a barrier mutation around the opening of the
// request's main snapshot, and fires it only for the first handle so a control
// read-back cannot trigger it again.
//
// beforeOpen commits while the snapshot is still unopened — the window the
// pre-EN-1867 executor read its definition in, because it read the live store
// before any handle existed. afterOpen commits once the snapshot is fixed.
// The two hooks bracket the ordering under test, so a definition sourced from
// either side of the snapshot is observable.
//
// The store is embedded rather than held in a field: the type then satisfies
// both the handle opener and dal.PebbleGetter, so this harness compiles against
// either read ordering and a mutation experiment needs no test-side edits.
type mutatingQueryHandleStore struct {
	*dal.Store

	beforeOpen func()
	afterOpen  func()
	once       sync.Once
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
	first := false
	s.once.Do(func() { first = true })

	if first && s.beforeOpen != nil {
		s.beforeOpen()
	}

	handle, err := s.Store.NewReadHandle()
	if err != nil {
		return nil, err
	}

	if first && s.afterOpen != nil {
		s.afterOpen()
	}

	return handle, nil
}

func TestExecute_ReadsDefinitionAndLedgerFromMainSnapshot(t *testing.T) {
	t.Parallel()

	for _, mutation := range []string{"prepared query deleted", "ledger deleted"} {
		t.Run(mutation, func(t *testing.T) {
			t.Parallel()

			store := newTestStore(t)
			registerLedger(t, store, "l")

			rs := newTestReadStore(t)
			attrs := attributes.New()
			seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, nil)

			opener := &mutatingQueryHandleStore{
				Store: store,
				afterOpen: func() {
					batch := store.OpenWriteSession()
					switch mutation {
					case "prepared query deleted":
						require.NoError(t, attrs.PreparedQuery.Delete(batch, domain.PreparedQueryKey{LedgerName: "l", Name: "q"}.Bytes()))
					case "ledger deleted":
						require.NoError(t, state.SaveLedger(batch, "l", &commonpb.LedgerInfo{
							Name:      "l",
							DeletedAt: &commonpb.Timestamp{},
						}))
					default:
						t.Fatalf("unknown mutation %q", mutation)
					}
					require.NoError(t, batch.Commit())
				},
			}

			_, err := query.Execute(
				t.Context(), rs, opener, attrs.Volume, attrs.PreparedQuery, attrs.Index,
				&servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q"}, nil, nil,
			)
			require.NoError(t, err,
				"definition and schema reads must stay on the handle opened before the live mutation")

			// NoError alone would also hold for a live read that happened not to
			// fail. Name the branch: a live read after the mutation surfaces the
			// not-found error for whichever object was removed.
			var pqNotFound *domain.ErrPreparedQueryNotFound
			require.NotErrorAs(t, err, &pqNotFound)

			var ledgerNotFound *domain.ErrLedgerNotFound
			require.NotErrorAs(t, err, &ledgerNotFound)
		})
	}
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

// The alignment decision must follow the definition the request read through
// its own snapshot. A filter replacement or a deletion committed after that
// snapshot belongs to a later state; compiling it against the snapshot's
// entities would tear the result the other way round.
//
// This guards the forward direction only, and deliberately so: a barrier that
// commits after the snapshot opens cannot distinguish snapshot-sourced from
// live-sourced lookups, because the pre-EN-1867 executor read the live store
// *before* any handle existed and so saw the pre-mutation state too. Both this
// table and TestExecute_ReadsDefinitionAndLedgerFromMainSnapshot pass against
// that ordering; TestExecute_DefinitionCommittedBeforeSnapshotIsObserved is the
// EN-1867 regression guard. What this table does catch is a future change that
// re-reads the definition from a later snapshot mid-request.
//
// Alignment is the discriminator because it is a pure function of the
// definition (AlignmentOwed) with an externally visible consequence: against a
// permanently lagging read store, a definition that reads an index leaf blocks
// on the fold and one that does not returns immediately. Asserting the wait
// therefore names which definition drove compilation, which a bare NoError
// cannot.
func TestExecute_DefinitionMutationAfterSnapshotIsNotObserved(t *testing.T) {
	t.Parallel()

	alignedFilter := func() *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field:     &commonpb.FieldRef{Metadata: "tier"},
				Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
			},
		}}
	}

	for _, tc := range []struct {
		name     string
		seeded   *commonpb.QueryFilter
		replaced *commonpb.QueryFilter
		deleted  bool
		wantWait bool
	}{
		{
			name:     "unaligned definition, aligned replacement committed after the snapshot",
			seeded:   nil,
			replaced: alignedFilter(),
			wantWait: false,
		},
		{
			name:     "aligned definition, unaligned replacement committed after the snapshot",
			seeded:   alignedFilter(),
			replaced: nil,
			wantWait: true,
		},
		{
			name:     "aligned definition, deletion committed after the snapshot",
			seeded:   alignedFilter(),
			deleted:  true,
			wantWait: true,
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
			key := domain.PreparedQueryKey{LedgerName: "l", Name: "q"}.Bytes()
			seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, tc.seeded)

			opener := &mutatingQueryHandleStore{
				Store: store,
				afterOpen: func() {
					batch := store.OpenWriteSession()
					if tc.deleted {
						require.NoError(t, attrs.PreparedQuery.Delete(batch, key))
					} else {
						_, err := attrs.PreparedQuery.Set(batch, key, &commonpb.PreparedQuery{
							Name:   "q",
							Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
							Filter: tc.replaced,
						})
						require.NoError(t, err)
					}

					require.NoError(t, batch.Commit())
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			_, err := query.Execute(
				ctx, rs, opener, attrs.Volume, attrs.PreparedQuery, attrs.Index,
				&servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q"}, nil, nil,
			)

			// Control: the barrier mutation is a committed write, so a handle
			// opened after the request sees it. Without this the subtests would
			// also pass if afterOpen silently wrote nothing.
			assertLivePreparedQuery(t, store, attrs, tc.deleted, tc.replaced)

			var notFound *domain.ErrPreparedQueryNotFound
			require.NotErrorAs(t, err, &notFound,
				"the definition must come from the request's own snapshot, where it still exists")

			if tc.wantWait {
				require.ErrorIs(t, err, context.DeadlineExceeded,
					"the snapshot's definition reads an index leaf, so alignment is still owed")

				return
			}

			require.NoError(t, err,
				"the snapshot's definition reads no index leaf, so no fold wait is owed")
		})
	}
}

// assertLivePreparedQuery reads the definition back through a fresh handle and
// proves the barrier mutation reached the store.
func assertLivePreparedQuery(
	t *testing.T,
	store *dal.Store,
	attrs *attributes.Attributes,
	deleted bool,
	replaced *commonpb.QueryFilter,
) {
	t.Helper()

	control, err := store.NewReadHandle()
	require.NoError(t, err)

	defer func() { _ = control.Close() }()

	live, err := query.ReadPreparedQuery(t.Context(), attrs.PreparedQuery, control, "l", "q")
	require.NoError(t, err)

	if deleted {
		require.Nil(t, live, "the barrier deletion must be committed")

		return
	}

	require.NotNil(t, live, "the barrier replacement must be committed")

	if replaced == nil {
		require.Nil(t, live.GetFilter(), "the barrier replacement must clear the filter")

		return
	}

	require.Equal(t, replaced.String(), live.GetFilter().String(),
		"the barrier replacement must be committed")
}

// EN-1867 regression guard. The executor once read the ledger schema and the
// prepared-query definition from the live store and only afterwards opened the
// request's snapshot, so a commit landing in that window produced a result
// assembled from an old definition and a newer main-store view.
//
// The barrier commits before the snapshot opens, which is exactly that window.
// A snapshot-sourced definition therefore reflects the mutation; a live-sourced
// one reflects the state before it. The two disagree observably because the
// seeded and replacement filters differ in whether they read an index leaf, and
// the read store here never catches up: the compiled definition either waits on
// the fold or it does not. The deletion case separates the same way — the
// snapshot cannot see a definition removed before it was taken.
//
// Each expectation is therefore the opposite of what the pre-EN-1867 ordering
// produces, which is what makes these subtests a guard rather than a
// description.
func TestExecute_DefinitionCommittedBeforeSnapshotIsObserved(t *testing.T) {
	t.Parallel()

	alignedFilter := func() *commonpb.QueryFilter {
		return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{
			Field: &commonpb.FieldCondition{
				Field:     &commonpb.FieldRef{Metadata: "tier"},
				Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
			},
		}}
	}

	const (
		outcomeWait     = "waits on the fold"
		outcomeOK       = "returns without waiting"
		outcomeNotFound = "reports the prepared query as missing"
	)

	for _, tc := range []struct {
		name     string
		seeded   *commonpb.QueryFilter
		replaced *commonpb.QueryFilter
		deleted  bool
		want     string
	}{
		{
			// Live-sourced definition: unaligned, returns immediately.
			name:     "unaligned definition replaced by an aligned one before the snapshot",
			seeded:   nil,
			replaced: alignedFilter(),
			want:     outcomeWait,
		},
		{
			// Live-sourced definition: aligned, blocks on the fold.
			name:     "aligned definition replaced by an unaligned one before the snapshot",
			seeded:   alignedFilter(),
			replaced: nil,
			want:     outcomeOK,
		},
		{
			// Live-sourced definition: still present, blocks on the fold.
			name:    "aligned definition deleted before the snapshot",
			seeded:  alignedFilter(),
			deleted: true,
			want:    outcomeNotFound,
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
			key := domain.PreparedQueryKey{LedgerName: "l", Name: "q"}.Bytes()
			seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, tc.seeded)

			opener := &mutatingQueryHandleStore{
				Store: store,
				beforeOpen: func() {
					batch := store.OpenWriteSession()
					if tc.deleted {
						require.NoError(t, attrs.PreparedQuery.Delete(batch, key))
					} else {
						_, err := attrs.PreparedQuery.Set(batch, key, &commonpb.PreparedQuery{
							Name:   "q",
							Target: commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
							Filter: tc.replaced,
						})
						require.NoError(t, err)
					}

					require.NoError(t, batch.Commit())
				},
			}

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()

			_, err := query.Execute(
				ctx, rs, opener, attrs.Volume, attrs.PreparedQuery, attrs.Index,
				&servicepb.ExecutePreparedQueryRequest{Ledger: "l", QueryName: "q"}, nil, nil,
			)

			// Control: without a committed barrier write the subtest would pass
			// on the seeded definition alone and assert nothing about ordering.
			assertLivePreparedQuery(t, store, attrs, tc.deleted, tc.replaced)

			var notFound *domain.ErrPreparedQueryNotFound

			switch tc.want {
			case outcomeWait:
				require.ErrorIs(t, err, context.DeadlineExceeded,
					"the snapshot's definition reads an index leaf; a live read would have seen the unaligned one and returned")

			case outcomeOK:
				require.NoError(t, err,
					"the snapshot's definition reads no index leaf; a live read would have seen the aligned one and waited")

			case outcomeNotFound:
				require.ErrorAs(t, err, &notFound,
					"the snapshot cannot see a definition deleted before it was taken; a live read would have found it and waited")

			default:
				t.Fatalf("unknown outcome %q", tc.want)
			}
		})
	}
}
