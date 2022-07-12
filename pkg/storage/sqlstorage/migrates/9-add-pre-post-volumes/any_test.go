package add_pre_post_volumes_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
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
				Amount:      100,
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {},
			},
			"bank": {
				"USD": {},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Output: 100,
				},
			},
			"bank": {
				"USD": {
					Input: 100,
				},
			},
		},
	},
	{
		postings: core.Postings{
			{
				Source:      "world",
				Destination: "bank2",
				Amount:      100,
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Output: 100,
				},
			},
			"bank2": {
				"USD": {},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Output: 200,
				},
			},
			"bank2": {
				"USD": {
					Input: 100,
				},
			},
		},
	},
	{
		postings: core.Postings{
			{
				Source:      "world",
				Destination: "bank",
				Amount:      100,
				Asset:       "USD",
			},
			{
				Source:      "world",
				Destination: "bank2",
				Amount:      100,
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Output: 200,
				},
			},
			"bank": {
				"USD": {
					Input: 100,
				},
			},
			"bank2": {
				"USD": {
					Input: 100,
				},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Output: 400,
				},
			},
			"bank2": {
				"USD": {
					Input: 200,
				},
			},
			"bank": {
				"USD": {
					Input: 200,
				},
			},
		},
	},
	{
		postings: core.Postings{
			{
				Source:      "bank",
				Destination: "user:1",
				Amount:      10,
				Asset:       "USD",
			},
			{
				Source:      "bank",
				Destination: "user:2",
				Amount:      90,
				Asset:       "USDT",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"bank": {
				"USD": {
					Input: 200,
				},
				"USDT": {},
			},
			"user:1": {
				"USD": {},
			},
			"user:2": {
				"USDT": {},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"bank": {
				"USD": {
					Input:  200,
					Output: 10,
				},
				"USDT": {
					Output: 90,
				},
			},
			"user:1": {
				"USD": {
					Input: 10,
				},
			},
			"user:2": {
				"USDT": {
					Input: 90,
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

	schema := store.(*sqlstorage.Store).Schema()

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

		_, err = schema.ExecContext(context.Background(),
			`INSERT INTO log (id, data, type, date) VALUES (?, ?, ?, ?)`,
			i, string(txData), core.NewTransactionType, now)
		require.NoError(t, err)
	}

	txs, err := store.GetTransactions(context.Background(), *storage.NewTransactionsQuery())
	require.NoError(t, err)
	require.Len(t, txs.Data, len(testCases))

	sqlTx, err := schema.BeginTx(context.Background(), &sql.TxOptions{})
	require.NoError(t, err)

	require.NoError(t, add_pre_post_volumes.Upgrade(context.Background(), schema, sqlTx))
	require.NoError(t, sqlTx.Commit())

	for i, tc := range testCases {
		tx, err := store.GetTransaction(context.Background(), uint64(i))
		require.NoError(t, err)
		require.Equal(t, tc.expectedPreCommitVolumes, tx.PreCommitVolumes)
		require.Equal(t, tc.expectedPostCommitVolumes, tx.PostCommitVolumes)
	}

}
