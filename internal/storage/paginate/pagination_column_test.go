package paginate_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
)

func TestColumnPagination(t *testing.T) {
	t.Parallel()

	pgServer := pgtesting.NewPostgresDatabase(t)
	db, err := storage.OpenSQLDB(storage.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
	})
	require.NoError(t, err)

	_, err = db.Exec(`
		CREATE TABLE "models" (id int, pair boolean);
	`)
	require.NoError(t, err)

	type model struct {
		ID   *paginate.BigInt `bun:"id,type:numeric"`
		Pair bool             `bun:"pair"`
	}

	models := make([]model, 0)
	for i := 0; i < 100; i++ {
		models = append(models, model{
			ID:   (*paginate.BigInt)(big.NewInt(int64(i))),
			Pair: i%2 == 0,
		})
	}

	_, err = db.NewInsert().
		Model(&models).
		Exec(context.Background())
	require.NoError(t, err)

	type testCase struct {
		name                  string
		query                 paginate.ColumnPaginatedQuery[bool]
		expectedNext          *paginate.ColumnPaginatedQuery[bool]
		expectedPrevious      *paginate.ColumnPaginatedQuery[bool]
		expectedNumberOfItems int64
	}
	testCases := []testCase{
		{
			name: "asc first page",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    paginate.OrderAsc,
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(10)),
				Order:        paginate.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page using next cursor",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(10)),
				Order:        paginate.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedPrevious: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				Order:        paginate.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
				PaginationID: big.NewInt(int64(10)),
				Reverse:      true,
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(20)),
				Order:        paginate.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc last page using next cursor",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(90)),
				Order:        paginate.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedPrevious: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				Order:        paginate.OrderAsc,
				PaginationID: big.NewInt(int64(90)),
				Bottom:       big.NewInt(int64(0)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    paginate.OrderDesc,
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        paginate.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc second page using next cursor",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        paginate.OrderDesc,
			},
			expectedPrevious: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        paginate.OrderDesc,
				Reverse:      true,
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(79)),
				Order:        paginate.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc last page using next cursor",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(9)),
				Order:        paginate.OrderDesc,
			},
			expectedPrevious: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(9)),
				Order:        paginate.OrderDesc,
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc first page using previous cursor",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(0)),
				Column:       "id",
				PaginationID: big.NewInt(int64(10)),
				Order:        paginate.OrderAsc,
				Reverse:      true,
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(0)),
				Column:       "id",
				PaginationID: big.NewInt(int64(10)),
				Order:        paginate.OrderAsc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page using previous cursor",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        paginate.OrderDesc,
				Reverse:      true,
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        paginate.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc first page with filter",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    paginate.OrderAsc,
				Options:  true,
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(20)),
				Order:        paginate.OrderAsc,
				Options:      true,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page with filter",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(20)),
				Order:        paginate.OrderAsc,
				Options:      true,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(40)),
				Order:        paginate.OrderAsc,
				Options:      true,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedPrevious: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(20)),
				Order:        paginate.OrderAsc,
				Options:      true,
				Bottom:       big.NewInt(int64(0)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page with filter",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    paginate.OrderDesc,
				Options:  true,
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(78)),
				Order:        paginate.OrderDesc,
				Options:      true,
				Bottom:       big.NewInt(int64(98)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc second page with filter",
			query: paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(78)),
				Order:        paginate.OrderDesc,
				Options:      true,
				Bottom:       big.NewInt(int64(98)),
			},
			expectedNext: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(58)),
				Order:        paginate.OrderDesc,
				Options:      true,
				Bottom:       big.NewInt(int64(98)),
			},
			expectedPrevious: &paginate.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(78)),
				Order:        paginate.OrderDesc,
				Options:      true,
				Bottom:       big.NewInt(int64(98)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			models := make([]model, 0)
			query := db.NewSelect().Model(&models).Column("id")
			if tc.query.Options {
				query = query.Where("pair = ?", true)
			}
			cursor, err := paginate.UsingColumn[bool, model](context.Background(), query, tc.query)
			require.NoError(t, err)

			if tc.expectedNext == nil {
				require.Empty(t, cursor.Next)
			} else {
				require.NotEmpty(t, cursor.Next)

				q := paginate.ColumnPaginatedQuery[bool]{}
				require.NoError(t, paginate.UnmarshalCursor(cursor.Next, &q))
				require.EqualValues(t, *tc.expectedNext, q)
			}

			if tc.expectedPrevious == nil {
				require.Empty(t, cursor.Previous)
			} else {
				require.NotEmpty(t, cursor.Previous)

				q := paginate.ColumnPaginatedQuery[bool]{}
				require.NoError(t, paginate.UnmarshalCursor(cursor.Previous, &q))
				require.EqualValues(t, *tc.expectedPrevious, q)
			}
		})
	}
}
