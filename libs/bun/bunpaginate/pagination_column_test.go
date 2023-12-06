package bunpaginate_test

import (
	"context"
	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"
	bunpaginate2 "github.com/formancehq/stack/libs/go-libs/bun/bunpaginate"
	"math/big"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
)

func TestColumnPagination(t *testing.T) {
	t.Parallel()

	pgServer := pgtesting.NewPostgresDatabase(t)
	db, err := bunconnect.OpenSQLDB(bunconnect.ConnectionOptions{
		DatabaseSourceName: pgServer.ConnString(),
		Debug:              testing.Verbose(),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = db.Close()
	})

	_, err = db.Exec(`
		CREATE TABLE "models" (id int, pair boolean);
	`)
	require.NoError(t, err)

	type model struct {
		ID   *bunpaginate2.BigInt `bun:"id,type:numeric"`
		Pair bool                 `bun:"pair"`
	}

	models := make([]model, 0)
	for i := 0; i < 100; i++ {
		models = append(models, model{
			ID:   (*bunpaginate2.BigInt)(big.NewInt(int64(i))),
			Pair: i%2 == 0,
		})
	}

	_, err = db.NewInsert().
		Model(&models).
		Exec(context.Background())
	require.NoError(t, err)

	type testCase struct {
		name                  string
		query                 bunpaginate2.ColumnPaginatedQuery[bool]
		expectedNext          *bunpaginate2.ColumnPaginatedQuery[bool]
		expectedPrevious      *bunpaginate2.ColumnPaginatedQuery[bool]
		expectedNumberOfItems int64
	}
	testCases := []testCase{
		{
			name: "asc first page",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    bunpaginate2.OrderAsc,
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(10)),
				Order:        bunpaginate2.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page using next cursor",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(10)),
				Order:        bunpaginate2.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedPrevious: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				Order:        bunpaginate2.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
				PaginationID: big.NewInt(int64(10)),
				Reverse:      true,
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(20)),
				Order:        bunpaginate2.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc last page using next cursor",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(90)),
				Order:        bunpaginate2.OrderAsc,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedPrevious: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				Order:        bunpaginate2.OrderAsc,
				PaginationID: big.NewInt(int64(90)),
				Bottom:       big.NewInt(int64(0)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    bunpaginate2.OrderDesc,
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        bunpaginate2.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc second page using next cursor",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        bunpaginate2.OrderDesc,
			},
			expectedPrevious: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        bunpaginate2.OrderDesc,
				Reverse:      true,
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(79)),
				Order:        bunpaginate2.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc last page using next cursor",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(9)),
				Order:        bunpaginate2.OrderDesc,
			},
			expectedPrevious: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(9)),
				Order:        bunpaginate2.OrderDesc,
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc first page using previous cursor",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(0)),
				Column:       "id",
				PaginationID: big.NewInt(int64(10)),
				Order:        bunpaginate2.OrderAsc,
				Reverse:      true,
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(0)),
				Column:       "id",
				PaginationID: big.NewInt(int64(10)),
				Order:        bunpaginate2.OrderAsc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page using previous cursor",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        bunpaginate2.OrderDesc,
				Reverse:      true,
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Bottom:       big.NewInt(int64(99)),
				Column:       "id",
				PaginationID: big.NewInt(int64(89)),
				Order:        bunpaginate2.OrderDesc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc first page with filter",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    bunpaginate2.OrderAsc,
				Options:  true,
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(20)),
				Order:        bunpaginate2.OrderAsc,
				Options:      true,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page with filter",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(20)),
				Order:        bunpaginate2.OrderAsc,
				Options:      true,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(40)),
				Order:        bunpaginate2.OrderAsc,
				Options:      true,
				Bottom:       big.NewInt(int64(0)),
			},
			expectedPrevious: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(20)),
				Order:        bunpaginate2.OrderAsc,
				Options:      true,
				Bottom:       big.NewInt(int64(0)),
				Reverse:      true,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc first page with filter",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize: 10,
				Column:   "id",
				Order:    bunpaginate2.OrderDesc,
				Options:  true,
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(78)),
				Order:        bunpaginate2.OrderDesc,
				Options:      true,
				Bottom:       big.NewInt(int64(98)),
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "desc second page with filter",
			query: bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(78)),
				Order:        bunpaginate2.OrderDesc,
				Options:      true,
				Bottom:       big.NewInt(int64(98)),
			},
			expectedNext: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(58)),
				Order:        bunpaginate2.OrderDesc,
				Options:      true,
				Bottom:       big.NewInt(int64(98)),
			},
			expectedPrevious: &bunpaginate2.ColumnPaginatedQuery[bool]{
				PageSize:     10,
				Column:       "id",
				PaginationID: big.NewInt(int64(78)),
				Order:        bunpaginate2.OrderDesc,
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
			cursor, err := bunpaginate2.UsingColumn[bool, model](context.Background(), query, tc.query)
			require.NoError(t, err)

			if tc.expectedNext == nil {
				require.Empty(t, cursor.Next)
			} else {
				require.NotEmpty(t, cursor.Next)

				q := bunpaginate2.ColumnPaginatedQuery[bool]{}
				require.NoError(t, bunpaginate2.UnmarshalCursor(cursor.Next, &q))
				require.EqualValues(t, *tc.expectedNext, q)
			}

			if tc.expectedPrevious == nil {
				require.Empty(t, cursor.Previous)
			} else {
				require.NotEmpty(t, cursor.Previous)

				q := bunpaginate2.ColumnPaginatedQuery[bool]{}
				require.NoError(t, bunpaginate2.UnmarshalCursor(cursor.Previous, &q))
				require.EqualValues(t, *tc.expectedPrevious, q)
			}
		})
	}
}
