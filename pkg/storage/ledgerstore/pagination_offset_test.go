package ledgerstore_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/pkg/storage/ledgerstore"
	"github.com/formancehq/ledger/pkg/storage/utils"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
)

func TestOffsetPagination(t *testing.T) {

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
		query                 ledgerstore.OffsetPaginatedQuery[bool]
		expectedNext          *ledgerstore.OffsetPaginatedQuery[bool]
		expectedPrevious      *ledgerstore.OffsetPaginatedQuery[bool]
		expectedNumberOfItems uint64
	}
	testCases := []testCase{
		{
			name: "asc first page",
			query: ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    ledgerstore.OrderAsc,
			},
			expectedNext: &ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   10,
				Order:    ledgerstore.OrderAsc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page using next cursor",
			query: ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   10,
				Order:    ledgerstore.OrderAsc,
			},
			expectedPrevious: &ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    ledgerstore.OrderAsc,
				Offset:   0,
			},
			expectedNext: &ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    ledgerstore.OrderAsc,
				Offset:   20,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc last page using next cursor",
			query: ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   90,
				Order:    ledgerstore.OrderAsc,
			},
			expectedPrevious: &ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    ledgerstore.OrderAsc,
				Offset:   80,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc last page partial",
			query: ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   95,
				Order:    ledgerstore.OrderAsc,
			},
			expectedPrevious: &ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    ledgerstore.OrderAsc,
				Offset:   85,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc fist page partial",
			query: ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   5,
				Order:    ledgerstore.OrderAsc,
			},
			expectedPrevious: &ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    ledgerstore.OrderAsc,
				Offset:   0,
			},
			expectedNext: &ledgerstore.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    ledgerstore.OrderAsc,
				Offset:   15,
			},
			expectedNumberOfItems: 10,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			query := db.NewSelect().Model(&models).Column("id")
			if tc.query.Filters {
				query = query.Where("pair = ?", true)
			}
			cursor, err := ledgerstore.UsingOffset(
				context.Background(),
				query,
				tc.query,
				func(t *model, scanner interface{ Scan(args ...any) error }) error {
					return scanner.Scan(&t.ID)
				})
			require.NoError(t, err)

			if tc.expectedNext == nil {
				require.Empty(t, cursor.Next)
			} else {
				require.NotEmpty(t, cursor.Next)

				q := ledgerstore.OffsetPaginatedQuery[bool]{}
				require.NoError(t, ledgerstore.UnmarshalCursor(cursor.Next, &q))
				require.EqualValues(t, *tc.expectedNext, q)
			}

			if tc.expectedPrevious == nil {
				require.Empty(t, cursor.Previous)
			} else {
				require.NotEmpty(t, cursor.Previous)

				q := ledgerstore.OffsetPaginatedQuery[bool]{}
				require.NoError(t, ledgerstore.UnmarshalCursor(cursor.Previous, &q))
				require.EqualValues(t, *tc.expectedPrevious, q)
			}
		})
	}
}
