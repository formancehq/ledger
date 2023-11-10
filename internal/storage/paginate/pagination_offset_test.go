package paginate_test

import (
	"context"
	"testing"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/formancehq/ledger/internal/storage/paginate"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/stretchr/testify/require"
)

func TestOffsetPagination(t *testing.T) {
	t.Parallel()

	pgServer := pgtesting.NewPostgresDatabase(t)
	db, err := sqlutils.OpenSQLDB(sqlutils.ConnectionOptions{
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
		query                 paginate.OffsetPaginatedQuery[bool]
		expectedNext          *paginate.OffsetPaginatedQuery[bool]
		expectedPrevious      *paginate.OffsetPaginatedQuery[bool]
		expectedNumberOfItems uint64
	}
	testCases := []testCase{
		{
			name: "asc first page",
			query: paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    paginate.OrderAsc,
			},
			expectedNext: &paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   10,
				Order:    paginate.OrderAsc,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc second page using next cursor",
			query: paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   10,
				Order:    paginate.OrderAsc,
			},
			expectedPrevious: &paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    paginate.OrderAsc,
				Offset:   0,
			},
			expectedNext: &paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    paginate.OrderAsc,
				Offset:   20,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc last page using next cursor",
			query: paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   90,
				Order:    paginate.OrderAsc,
			},
			expectedPrevious: &paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    paginate.OrderAsc,
				Offset:   80,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc last page partial",
			query: paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   95,
				Order:    paginate.OrderAsc,
			},
			expectedPrevious: &paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    paginate.OrderAsc,
				Offset:   85,
			},
			expectedNumberOfItems: 10,
		},
		{
			name: "asc fist page partial",
			query: paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Offset:   5,
				Order:    paginate.OrderAsc,
			},
			expectedPrevious: &paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    paginate.OrderAsc,
				Offset:   0,
			},
			expectedNext: &paginate.OffsetPaginatedQuery[bool]{
				PageSize: 10,
				Order:    paginate.OrderAsc,
				Offset:   15,
			},
			expectedNumberOfItems: 10,
		},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			query := db.NewSelect().Model(&models).Column("id")
			if tc.query.Options {
				query = query.Where("pair = ?", true)
			}
			cursor, err := paginate.UsingOffset[bool, model](
				context.Background(),
				query,
				tc.query)
			require.NoError(t, err)

			if tc.expectedNext == nil {
				require.Empty(t, cursor.Next)
			} else {
				require.NotEmpty(t, cursor.Next)

				q := paginate.OffsetPaginatedQuery[bool]{}
				require.NoError(t, paginate.UnmarshalCursor(cursor.Next, &q))
				require.EqualValues(t, *tc.expectedNext, q)
			}

			if tc.expectedPrevious == nil {
				require.Empty(t, cursor.Previous)
			} else {
				require.NotEmpty(t, cursor.Previous)

				q := paginate.OffsetPaginatedQuery[bool]{}
				require.NoError(t, paginate.UnmarshalCursor(cursor.Previous, &q))
				require.EqualValues(t, *tc.expectedPrevious, q)
			}
		})
	}
}
