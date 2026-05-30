package ledger

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v5/pkg/query"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
)

func TestAccountsFromQuery(t *testing.T) {
	t.Parallel()

	type testCase struct {
		name             string
		builder          query.Builder
		expectedAccounts []string
	}

	for _, tc := range []testCase{
		{
			name:             "nil builder",
			builder:          nil,
			expectedAccounts: nil,
		},
		{
			name:             "single address match",
			builder:          query.Match("address", "users:001"),
			expectedAccounts: []string{"users:001"},
		},
		{
			name:             "single account match",
			builder:          query.Match("account", "users:001"),
			expectedAccounts: []string{"users:001"},
		},
		{
			name:             "address match alongside other filters",
			builder:          query.And(query.Match("address", "users:001"), query.Match("balance", 100)),
			expectedAccounts: []string{"users:001"},
		},
		{
			name:             "no address filter",
			builder:          query.Match("balance", 100),
			expectedAccounts: nil,
		},
		{
			name:             "several address matches",
			builder:          query.Or(query.Match("address", "users:001"), query.Match("address", "users:002")),
			expectedAccounts: []string{"users:001", "users:002"},
		},
		{
			name:             "in operator with several accounts",
			builder:          query.In("address", []any{"users:001", "users:002"}),
			expectedAccounts: []string{"users:001", "users:002"},
		},
		{
			name:             "duplicates across aliases are deduped and sorted",
			builder:          query.Or(query.Match("account", "users:002"), query.Match("address", "users:002"), query.Match("address", "users:001")),
			expectedAccounts: []string{"users:001", "users:002"},
		},
		{
			name:             "address match with non-string value",
			builder:          query.Match("address", 42),
			expectedAccounts: nil,
		},
		{
			name:             "address with pattern operator is ignored",
			builder:          query.Like("address", "users:%"),
			expectedAccounts: nil,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			require.Equal(t, tc.expectedAccounts, accountsFromQuery(tc.builder))
		})
	}
}

func TestResourceQueryFromPaginatedQuery(t *testing.T) {
	t.Parallel()

	builder := query.Match("address", "users:001")

	for _, tc := range []struct {
		name  string
		query common.PaginatedQuery[ledger.GetVolumesOptions]
	}{
		{
			name: "initial",
			query: common.InitialPaginatedQuery[ledger.GetVolumesOptions]{
				Options: common.ResourceQuery[ledger.GetVolumesOptions]{Builder: builder},
			},
		},
		{
			name: "offset",
			query: common.OffsetPaginatedQuery[ledger.GetVolumesOptions]{
				InitialPaginatedQuery: common.InitialPaginatedQuery[ledger.GetVolumesOptions]{
					Options: common.ResourceQuery[ledger.GetVolumesOptions]{Builder: builder},
				},
			},
		},
		{
			name: "column",
			query: common.ColumnPaginatedQuery[ledger.GetVolumesOptions]{
				InitialPaginatedQuery: common.InitialPaginatedQuery[ledger.GetVolumesOptions]{
					Options: common.ResourceQuery[ledger.GetVolumesOptions]{Builder: builder},
				},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rq, ok := common.ResourceQueryFromPaginatedQuery[ledger.GetVolumesOptions](tc.query)
			require.True(t, ok)

			require.Equal(t, []string{"users:001"}, accountsFromQuery(rq.Builder))
		})
	}
}
