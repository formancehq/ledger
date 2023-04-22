package pagination_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/pagination"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/utils"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func ptr[T any](t T) *T {
	return &t
}

func TestColumnPagination(t *testing.T) {

	pgServer := pgtesting.NewPostgresDatabase(t)
	db, err := utils.OpenSQLDB(utils.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
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
		query                 storage.ColumnPaginatedQuery[bool]
		expectedNext          *storage.ColumnPaginatedQuery[bool]
		expectedPrevious      *storage.ColumnPaginatedQuery[bool]
		expectedNumberOfItems uint64
	}
	testCases := []testCase{
		{
			name: "asc first page",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    storage.OrderAsc,
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(10)),
				Order:        storage.OrderAsc,
				Bottom:       ptr(uint64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page using next cursor",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(10)),
				Order:        storage.OrderAsc,
				Bottom:       ptr(uint64(0)),
			},
			expectedPrevious: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				Order:        storage.OrderAsc,
				Bottom:       ptr(uint64(0)),
				PaginationID: ptr(uint64(10)),
				Reverse:      true,
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(20)),
				Order:        storage.OrderAsc,
				Bottom:       ptr(uint64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc last page using next cursor",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(90)),
				Order:        storage.OrderAsc,
				Bottom:       ptr(uint64(0)),
			},
			expectedPrevious: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				Order:        storage.OrderAsc,
				PaginationID: ptr(uint64(90)),
				Bottom:       ptr(uint64(0)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    storage.OrderDesc,
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(99)),
				Column:       "id",
				PaginationID: ptr(uint64(89)),
				Order:        storage.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc second page using next cursor",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(99)),
				Column:       "id",
				PaginationID: ptr(uint64(89)),
				Order:        storage.OrderDesc,
			},
			expectedPrevious: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(99)),
				Column:       "id",
				PaginationID: ptr(uint64(89)),
				Order:        storage.OrderDesc,
				Reverse:      true,
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(99)),
				Column:       "id",
				PaginationID: ptr(uint64(79)),
				Order:        storage.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc last page using next cursor",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(99)),
				Column:       "id",
				PaginationID: ptr(uint64(9)),
				Order:        storage.OrderDesc,
			},
			expectedPrevious: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(99)),
				Column:       "id",
				PaginationID: ptr(uint64(9)),
				Order:        storage.OrderDesc,
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc first page using previous cursor",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(0)),
				Column:       "id",
				PaginationID: ptr(uint64(10)),
				Order:        storage.OrderAsc,
				Reverse:      true,
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(0)),
				Column:       "id",
				PaginationID: ptr(uint64(10)),
				Order:        storage.OrderAsc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page using previous cursor",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(99)),
				Column:       "id",
				PaginationID: ptr(uint64(89)),
				Order:        storage.OrderDesc,
				Reverse:      true,
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       ptr(uint64(99)),
				Column:       "id",
				PaginationID: ptr(uint64(89)),
				Order:        storage.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc first page with filter",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    storage.OrderAsc,
				Filters:  true,
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(20)),
				Order:        storage.OrderAsc,
				Filters:      true,
				Bottom:       ptr(uint64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page with filter",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(20)),
				Order:        storage.OrderAsc,
				Filters:      true,
				Bottom:       ptr(uint64(0)),
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(40)),
				Order:        storage.OrderAsc,
				Filters:      true,
				Bottom:       ptr(uint64(0)),
			},
			expectedPrevious: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(20)),
				Order:        storage.OrderAsc,
				Filters:      true,
				Bottom:       ptr(uint64(0)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page with filter",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    storage.OrderDesc,
				Filters:  true,
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(78)),
				Order:        storage.OrderDesc,
				Filters:      true,
				Bottom:       ptr(uint64(98)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc second page with filter",
			query: storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(78)),
				Order:        storage.OrderDesc,
				Filters:      true,
				Bottom:       ptr(uint64(98)),
			},
			expectedNext: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(58)),
				Order:        storage.OrderDesc,
				Filters:      true,
				Bottom:       ptr(uint64(98)),
			},
			expectedPrevious: &storage.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: ptr(uint64(78)),
				Order:        storage.OrderDesc,
				Filters:      true,
				Bottom:       ptr(uint64(98)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			cursor, err := pagination.UsingColumn(
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

				q := storage.ColumnPaginatedQuery[bool]{}
				require.NoError(t, storage.UnmarshalCursor(cursor.Next, &q))
				require.EqualValues(t, *tc.expectedNext, q)
			}

			if tc.expectedPrevious == nil {
				require.Empty(t, cursor.Previous)
			} else {
				require.NotEmpty(t, cursor.Previous)

				q := storage.ColumnPaginatedQuery[bool]{}
				require.NoError(t, storage.UnmarshalCursor(cursor.Previous, &q))
				require.EqualValues(t, *tc.expectedPrevious, q)
			}
		})
	}
}
