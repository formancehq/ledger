package query_test

import (
	"fmt"
	"math"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
)

func pageSizeAccounts() []string {
	accounts := make([]string, 1002)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("account:%04d", i)
	}

	return accounts
}

// LIST's effective page size bounds collection and hydration in either
// direction, and the links resume at the normalized page boundary.
func TestExecute_ListPageSizeBounds(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		size uint32
		want int
	}{
		{name: "default", size: 0, want: 100},
		{name: "maximum", size: 1000, want: 1000},
		{name: "above maximum", size: 1001, want: 1000},
		{name: "uint32 maximum", size: math.MaxUint32, want: 1000},
	} {
		for _, reverse := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/reverse=%v", tc.name, reverse), func(t *testing.T) {
				t.Parallel()

				accounts := pageSizeAccounts()
				f := newReverseFixture(t, nil, accounts...)
				if reverse {
					slices.Reverse(accounts)
				}

				enriched := 0
				enricher := &query.EntityEnricher{
					EnrichAccount: func(_ dal.PebbleReader, _ string, address string) (*ledgerpb.Account, error) {
						enriched++

						return &ledgerpb.Account{Address: address}, nil
					},
				}
				execute := func(token string) *ledgerpb.PreparedQueryCursor {
					t.Helper()
					enriched = 0
					profile := &query.QueryProfile{}
					resp, err := query.Execute(t.Context(), newTestReadStore(t), f.store,
						f.attrs.Volume, f.attrs.PreparedQuery, f.attrs.Index,
						&servicepb.ExecutePreparedQueryRequest{
							Ledger: "l", QueryName: "q", PageSize: tc.size,
							Cursor: token, Reverse: reverse,
							Mode: ledgerpb.QueryMode_QUERY_MODE_LIST,
						}, profile, enricher)
					require.NoError(t, err)
					cursor := resp.GetCursor()
					require.NotNil(t, cursor)
					require.Equal(t, uint32(tc.want), cursor.GetPageSize())
					require.LessOrEqual(t, len(cursor.GetAccountData()), tc.want)
					require.Equal(t, len(cursor.GetAccountData()), enriched, "hydrate only the returned page")
					require.Equal(t, len(cursor.GetAccountData()), profile.ItemsCollected)

					return cursor
				}

				first := execute("")
				require.Equal(t, accounts[:tc.want], pageAddresses(first))
				require.Empty(t, first.GetPrevious())
				require.True(t, first.GetHasMore())
				require.Equal(t, pagecursor.Cursor{Key: accounts[tc.want-1]}, decodeLink(t, first.GetNext()))

				second := execute(first.GetNext())
				end := min(2*tc.want, len(accounts))
				require.Equal(t, accounts[tc.want:end], pageAddresses(second))
				require.Equal(t, end < len(accounts), second.GetHasMore())
				require.Equal(t, pagecursor.Cursor{Key: accounts[tc.want], Back: true}, decodeLink(t, second.GetPrevious()))
				if end == len(accounts) {
					require.Empty(t, second.GetNext())
				} else {
					require.Equal(t, pagecursor.Cursor{Key: accounts[end-1]}, decodeLink(t, second.GetNext()))
				}

				back := execute(second.GetPrevious())
				require.Equal(t, pageAddresses(first), pageAddresses(back))
				require.Equal(t, first.GetNext(), back.GetNext())
				require.Empty(t, back.GetPrevious())
			})
		}
	}
}

func TestExecute_AggregateIgnoresListPageSize(t *testing.T) {
	t.Parallel()

	for _, filter := range []*ledgerpb.QueryFilter{nil, usersPrefixFilter()} {
		t.Run(fmt.Sprintf("filtered=%v", filter != nil), func(t *testing.T) {
			t.Parallel()

			accounts := pageSizeAccounts()
			f := newReverseFixture(t, filter, accounts...)
			for _, size := range []uint32{1, 1001, math.MaxUint32} {
				resp, err := f.execute(&servicepb.ExecutePreparedQueryRequest{
					PageSize: size,
					Mode:     ledgerpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES,
					Parameters: map[string]*ledgerpb.ParameterValue{
						"prefix": {Value: &ledgerpb.ParameterValue_StringValue{StringValue: "account:"}},
					},
				})
				require.NoError(t, err)
				require.Nil(t, resp.GetCursor())
				volumes := resp.GetAggregate().GetVolumes()
				require.Len(t, volumes, 1)
				require.Equal(t, "USD/2", volumes[0].GetAsset())
				require.Equal(t, ledgerpb.NewUint256FromUint64(1002), volumes[0].GetInput(), "aggregate every matching account, pageSize=%d", size)
			}
		})
	}
}
