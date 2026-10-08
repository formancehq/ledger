package query_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// addressEnricher hydrates an account to its bare address, which is all the
// paging assertions read.
func addressEnricher() *query.EntityEnricher {
	return &query.EntityEnricher{
		EnrichAccount: func(_ dal.PebbleReader, _ string, address string) (*commonpb.Account, error) {
			return &commonpb.Account{Address: address}, nil
		},
	}
}

type reverseFixture struct {
	t     *testing.T
	store *dal.Store
	attrs *attributes.Attributes
}

func newReverseFixture(t *testing.T, filter *commonpb.QueryFilter, accounts ...string) *reverseFixture {
	t.Helper()

	store := newTestStore(t)
	registerLedger(t, store, "l")

	attrs := attributes.New()
	seedPreparedQuery(t, store, attrs, "l", "q", commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, filter)

	volumes := make([]seededVolume, len(accounts))
	for i, account := range accounts {
		volumes[i] = seededVolume{account: account, asset: "USD/2", input: 1}
	}
	seedVolumes(t, store, attrs, "l", volumes...)

	return &reverseFixture{t: t, store: store, attrs: attrs}
}

func (f *reverseFixture) execute(req *servicepb.ExecutePreparedQueryRequest) (*servicepb.ExecutePreparedQueryResponse, error) {
	f.t.Helper()

	if req.GetLedger() == "" {
		req.Ledger = "l"
	}
	req.QueryName = "q"

	return query.Execute(f.t.Context(), newTestReadStore(f.t), f.store,
		f.attrs.Volume, f.attrs.PreparedQuery, f.attrs.Index, req, nil, addressEnricher())
}

func (f *reverseFixture) page(req *servicepb.ExecutePreparedQueryRequest) *commonpb.PreparedQueryCursor {
	f.t.Helper()

	resp, err := f.execute(req)
	require.NoError(f.t, err)

	cursor := resp.GetCursor()
	require.NotNil(f.t, cursor, "LIST must answer with a cursor")

	return cursor
}

func pageAddresses(cursor *commonpb.PreparedQueryCursor) []string {
	out := make([]string, len(cursor.GetAccountData()))
	for i, account := range cursor.GetAccountData() {
		out[i] = account.GetAddress()
	}

	return out
}

func usersPrefixFilter() *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{
		Address: &commonpb.AddressMatch{
			Match: &commonpb.AddressMatch_ParamPrefix{ParamPrefix: "prefix"},
		},
	}}
}

func TestExecute_ReverseList(t *testing.T) {
	t.Parallel()

	accounts := []string{"bank", "users:1", "users:2", "users:3", "world"}
	usersParams := map[string]*commonpb.ParameterValue{
		"prefix": {Value: &commonpb.ParameterValue_StringValue{StringValue: "users:"}},
	}

	for _, tc := range []struct {
		name   string
		filter *commonpb.QueryFilter
		params map[string]*commonpb.ParameterValue
		pages  [][]string
	}{
		{
			name:  "nil filter",
			pages: [][]string{{"world", "users:3"}, {"users:2", "users:1"}, {"bank"}},
		},
		{
			name:   "parameterized filter",
			filter: usersPrefixFilter(),
			params: usersParams,
			pages:  [][]string{{"users:3", "users:2"}, {"users:1"}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newReverseFixture(t, tc.filter, accounts...)

			var next string
			for i, want := range tc.pages {
				cursor := f.page(&servicepb.ExecutePreparedQueryRequest{
					Parameters: tc.params,
					PageSize:   2,
					Cursor:     next,
					Reverse:    true,
				})

				require.Equal(t, want, pageAddresses(cursor), "page %d", i)

				last := i == len(tc.pages)-1
				require.Equal(t, !last, cursor.GetHasMore(), "page %d", i)
				require.Equal(t, !last, cursor.GetNext() != "", "page %d", i)

				next = cursor.GetNext()
			}
		})
	}
}

func TestExecute_ReverseListEmptyPage(t *testing.T) {
	t.Parallel()

	t.Run("no entities", func(t *testing.T) {
		t.Parallel()

		f := newReverseFixture(t, nil)

		cursor := f.page(&servicepb.ExecutePreparedQueryRequest{PageSize: 2, Reverse: true})
		require.Empty(t, cursor.GetAccountData())
		require.False(t, cursor.GetHasMore())
		require.Empty(t, cursor.GetNext())
	})

	t.Run("cursor at the lowest entity", func(t *testing.T) {
		t.Parallel()

		f := newReverseFixture(t, nil, "a", "b")

		first := f.page(&servicepb.ExecutePreparedQueryRequest{PageSize: 1, Reverse: true})
		require.Equal(t, []string{"b"}, pageAddresses(first))

		second := f.page(&servicepb.ExecutePreparedQueryRequest{PageSize: 1, Cursor: first.GetNext(), Reverse: true})
		require.Equal(t, []string{"a"}, pageAddresses(second))
		require.False(t, second.GetHasMore())

		// A forward cursor positioned on "a" resumes below it in reverse:
		// nothing is left.
		forward := f.page(&servicepb.ExecutePreparedQueryRequest{PageSize: 1})
		require.Equal(t, []string{"a"}, pageAddresses(forward))

		past := f.page(&servicepb.ExecutePreparedQueryRequest{PageSize: 1, Cursor: forward.GetNext(), Reverse: true})
		require.Empty(t, past.GetAccountData())
		require.False(t, past.GetHasMore())
		require.Empty(t, past.GetNext())
	})
}

func TestExecute_ForwardListStaysAscending(t *testing.T) {
	t.Parallel()

	f := newReverseFixture(t, nil, "a", "b", "c")

	cursor := f.page(&servicepb.ExecutePreparedQueryRequest{PageSize: 2})
	require.Equal(t, []string{"a", "b"}, pageAddresses(cursor))
	require.True(t, cursor.GetHasMore())

	cursor = f.page(&servicepb.ExecutePreparedQueryRequest{PageSize: 2, Cursor: cursor.GetNext()})
	require.Equal(t, []string{"c"}, pageAddresses(cursor))
	require.False(t, cursor.GetHasMore())
}

func TestExecute_ReverseAggregateRejected(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name   string
		filter *commonpb.QueryFilter
		ledger string
	}{
		{name: "nil filter"},
		{name: "filtered", filter: usersPrefixFilter()},
		{name: "unknown ledger", ledger: "missing"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			f := newReverseFixture(t, tc.filter, "a")

			_, err := f.execute(&servicepb.ExecutePreparedQueryRequest{
				Ledger:  tc.ledger,
				Mode:    commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
				Reverse: true,
			})

			var reverseErr *query.ErrQueryModeReverseUnsupported
			require.ErrorAs(t, err, &reverseErr)
			require.Equal(t, commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES, reverseErr.Mode)
		})
	}
}
