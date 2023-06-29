package ledgerstore_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/formancehq/stack/libs/go-libs/pointer"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func TestColumnPagination(t *testing.T) {
	t.Parallel()

	pgServer := pgtesting.NewPostgresDatabase(t)
	db, err := storage.OpenSQLDB(storage.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
		Trace:              testing.Verbose(),
	})
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE "models" (id int, pair boolean);
	`)
	require.NoError(t, err)

	type model struct {
		ID   uint64 `bun:"id"`
		Pair bool   `bun:"pair"`
	}

	models := make([]model, 0)
	for i := 0; i < 100; i++ {
		models = append(models, model{
			ID:   uint64(i),
			Pair: i%2 == 0,
		})
	}

	_, err = db.NewInsert().
		Model(&models).
		Exec(context.Background())
	require.NoError(t, err)

	type testCase struct {
		name                  string
		query                 ledgerstore.ColumnPaginatedQuery[bool]
		expectedNext          *ledgerstore.ColumnPaginatedQuery[bool]
		expectedPrevious      *ledgerstore.ColumnPaginatedQuery[bool]
		expectedNumberOfItems uint64
	}
	testCases := []testCase{
		{
			name: "asc first page",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    ledgerstore.OrderAsc,
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(10)),
				Order:        ledgerstore.OrderAsc,
				Bottom:       pointer.For(uint64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page using next cursor",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(10)),
				Order:        ledgerstore.OrderAsc,
				Bottom:       pointer.For(uint64(0)),
			},
			expectedPrevious: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				Order:        ledgerstore.OrderAsc,
				Bottom:       pointer.For(uint64(0)),
				PaginationID: pointer.For(uint64(10)),
				Reverse:      true,
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(20)),
				Order:        ledgerstore.OrderAsc,
				Bottom:       pointer.For(uint64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc last page using next cursor",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(90)),
				Order:        ledgerstore.OrderAsc,
				Bottom:       pointer.For(uint64(0)),
			},
			expectedPrevious: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				Order:        ledgerstore.OrderAsc,
				PaginationID: pointer.For(uint64(90)),
				Bottom:       pointer.For(uint64(0)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    ledgerstore.OrderDesc,
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(99)),
				Column:       "id",
				PaginationID: pointer.For(uint64(89)),
				Order:        ledgerstore.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc second page using next cursor",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(99)),
				Column:       "id",
				PaginationID: pointer.For(uint64(89)),
				Order:        ledgerstore.OrderDesc,
			},
			expectedPrevious: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(99)),
				Column:       "id",
				PaginationID: pointer.For(uint64(89)),
				Order:        ledgerstore.OrderDesc,
				Reverse:      true,
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(99)),
				Column:       "id",
				PaginationID: pointer.For(uint64(79)),
				Order:        ledgerstore.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc last page using next cursor",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(99)),
				Column:       "id",
				PaginationID: pointer.For(uint64(9)),
				Order:        ledgerstore.OrderDesc,
			},
			expectedPrevious: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(99)),
				Column:       "id",
				PaginationID: pointer.For(uint64(9)),
				Order:        ledgerstore.OrderDesc,
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc first page using previous cursor",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(0)),
				Column:       "id",
				PaginationID: pointer.For(uint64(10)),
				Order:        ledgerstore.OrderAsc,
				Reverse:      true,
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(0)),
				Column:       "id",
				PaginationID: pointer.For(uint64(10)),
				Order:        ledgerstore.OrderAsc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page using previous cursor",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(99)),
				Column:       "id",
				PaginationID: pointer.For(uint64(89)),
				Order:        ledgerstore.OrderDesc,
				Reverse:      true,
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       pointer.For(uint64(99)),
				Column:       "id",
				PaginationID: pointer.For(uint64(89)),
				Order:        ledgerstore.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc first page with filter",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    ledgerstore.OrderAsc,
				Filters:  true,
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(20)),
				Order:        ledgerstore.OrderAsc,
				Filters:      true,
				Bottom:       pointer.For(uint64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page with filter",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(20)),
				Order:        ledgerstore.OrderAsc,
				Filters:      true,
				Bottom:       pointer.For(uint64(0)),
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(40)),
				Order:        ledgerstore.OrderAsc,
				Filters:      true,
				Bottom:       pointer.For(uint64(0)),
			},
			expectedPrevious: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(20)),
				Order:        ledgerstore.OrderAsc,
				Filters:      true,
				Bottom:       pointer.For(uint64(0)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page with filter",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    ledgerstore.OrderDesc,
				Filters:  true,
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(78)),
				Order:        ledgerstore.OrderDesc,
				Filters:      true,
				Bottom:       pointer.For(uint64(98)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc second page with filter",
			query: ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(78)),
				Order:        ledgerstore.OrderDesc,
				Filters:      true,
				Bottom:       pointer.For(uint64(98)),
			},
			expectedNext: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(58)),
				Order:        ledgerstore.OrderDesc,
				Filters:      true,
				Bottom:       pointer.For(uint64(98)),
			},
			expectedPrevious: &ledgerstore.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: pointer.For(uint64(78)),
				Order:        ledgerstore.OrderDesc,
				Filters:      true,
				Bottom:       pointer.For(uint64(98)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cursor, err := ledgerstore.UsingColumn(
				context.Background(),
				func(filters bool, models *[]model) *bun.SelectQuery {
					query := db.NewSelect().Model(models).Column("id")
					if tc.query.Filters {
						query = query.Where("pair = ?", true)
					}
					return query
				},
				tc.query)
			require.NoError(t, err)

			if tc.expectedNext == nil {
				require.Empty(t, cursor.Next)
			} else {
				require.NotEmpty(t, cursor.Next)

				q := ledgerstore.ColumnPaginatedQuery[bool]{}
				require.NoError(t, ledgerstore.UnmarshalCursor(cursor.Next, &q))
				require.EqualValues(t, *tc.expectedNext, q)
			}

			if tc.expectedPrevious == nil {
				require.Empty(t, cursor.Previous)
			} else {
				require.NotEmpty(t, cursor.Previous)

				q := ledgerstore.ColumnPaginatedQuery[bool]{}
				require.NoError(t, ledgerstore.UnmarshalCursor(cursor.Previous, &q))
				require.EqualValues(t, *tc.expectedPrevious, q)
			}
		})
	}
}
