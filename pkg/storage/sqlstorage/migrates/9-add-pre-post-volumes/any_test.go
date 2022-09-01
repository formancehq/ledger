package add_pre_post_volumes_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	add_pre_post_volumes "github.com/numary/ledger/pkg/storage/sqlstorage/migrates/9-add-pre-post-volumes"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
)

type testCase struct {
	postings                  core.Postings
	expectedPreCommitVolumes  core.AccountsAssetsVolumes
	expectedPostCommitVolumes core.AccountsAssetsVolumes
}

var testCases = []testCase{
	{
		postings: core.Postings{
			{
				Source:      "world",
				Destination: "bank",
				Amount:      core.NewMonetaryInt(100),
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(0),
				},
			},
			"bank": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(100),
				},
			},
			"bank": {
				"USD": {
					Input:  core.NewMonetaryInt(100),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
	},
	{
		postings: core.Postings{
			{
				Source:      "world",
				Destination: "bank2",
				Amount:      core.NewMonetaryInt(100),
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(100),
				},
			},
			"bank2": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(200),
				},
			},
			"bank2": {
				"USD": {
					Input:  core.NewMonetaryInt(100),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
	},
	{
		postings: core.Postings{
			{
				Source:      "world",
				Destination: "bank",
				Amount:      core.NewMonetaryInt(100),
				Asset:       "USD",
			},
			{
				Source:      "world",
				Destination: "bank2",
				Amount:      core.NewMonetaryInt(100),
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(200),
				},
			},
			"bank": {
				"USD": {
					Input:  core.NewMonetaryInt(100),
					Output: core.NewMonetaryInt(0),
				},
			},
			"bank2": {
				"USD": {
					Input:  core.NewMonetaryInt(100),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(400),
				},
			},
			"bank2": {
				"USD": {
					Input:  core.NewMonetaryInt(200),
					Output: core.NewMonetaryInt(0),
				},
			},
			"bank": {
				"USD": {
					Input:  core.NewMonetaryInt(200),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
	},
	{
		postings: core.Postings{
			{
				Source:      "bank",
				Destination: "user:1",
				Amount:      core.NewMonetaryInt(10),
				Asset:       "USD",
			},
			{
				Source:      "bank",
				Destination: "user:2",
				Amount:      core.NewMonetaryInt(90),
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"bank": {
				"USD": {
					Input:  core.NewMonetaryInt(200),
					Output: core.NewMonetaryInt(0),
				},
			},
			"user:1": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(0),
				},
			},
			"user:2": {
				"USD": {
					Input:  core.NewMonetaryInt(0),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"bank": {
				"USD": {
					Input:  core.NewMonetaryInt(200),
					Output: core.NewMonetaryInt(100),
				},
			},
			"user:1": {
				"USD": {
					Input:  core.NewMonetaryInt(10),
					Output: core.NewMonetaryInt(0),
				},
			},
			"user:2": {
				"USD": {
					Input:  core.NewMonetaryInt(90),
					Output: core.NewMonetaryInt(0),
				},
			},
		},
	},
}

func TestMigrate9(t *testing.T) {
	driver, closeFunc, err := ledgertesting.StorageDriver()
	require.NoError(t, err)
	defer closeFunc()

	require.NoError(t, driver.Initialize(context.Background()))
	store, _, err := driver.GetStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)

	schema := store.Schema()

	migrations, err := sqlstorage.CollectMigrationFiles(sqlstorage.MigrationsFS)
	require.NoError(t, err)

	modified, err := sqlstorage.Migrate(context.Background(), schema, migrations[0:9]...)
	require.NoError(t, err)
	require.True(t, modified)

	now := time.Now()
	for i, tc := range testCases {
		txData, err := json.Marshal(struct {
			add_pre_post_volumes.Transaction
			Date time.Time `json:"timestamp"`
		}{
			Transaction: add_pre_post_volumes.Transaction{
				ID:       uint64(i),
				Postings: tc.postings,
			},
			Date: now,
		})
		require.NoError(t, err)

		ib := sqlbuilder.NewInsertBuilder()
		ib.InsertInto(schema.Table("log"))
		ib.Cols("id", "data", "type", "date")
		ib.Values(i, string(txData), core.NewTransactionType, now)
		sqlq, args := ib.BuildWithFlavor(schema.Flavor())

		_, err = schema.ExecContext(context.Background(), sqlq, args...)
		require.NoError(t, err)
	}

	count, err := store.CountTransactions(context.Background(), *ledger.NewTransactionsQuery())
	require.NoError(t, err)
	require.Equal(t, count, uint64(len(testCases)))

	sqlTx, err := schema.BeginTx(context.Background(), &sql.TxOptions{})
	require.NoError(t, err)

	require.NoError(t, add_pre_post_volumes.Upgrade(context.Background(), schema, sqlTx))
	require.NoError(t, sqlTx.Commit())

	for i, tc := range testCases {

		sb := sqlbuilder.NewSelectBuilder()
		sqlq, args := sb.
			From(schema.Table("transactions")).
			Select("pre_commit_volumes", "post_commit_volumes").
			Where(sb.E("id", i)).
			BuildWithFlavor(schema.Flavor())
		row := schema.QueryRowContext(context.Background(), sqlq, args...)
		require.NoError(t, row.Err())

		preCommitVolumes, postCommitVolumes := core.AccountsAssetsVolumes{}, core.AccountsAssetsVolumes{}
		require.NoError(t, row.Scan(&preCommitVolumes, &postCommitVolumes))

		require.Equal(t, tc.expectedPreCommitVolumes, preCommitVolumes)
		require.Equal(t, tc.expectedPostCommitVolumes, postCommitVolumes)
	}

}
