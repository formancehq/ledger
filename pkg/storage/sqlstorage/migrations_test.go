package sqlstorage_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var v0CreateTransaction = func(t *testing.T, store *sqlstorage.Store, tx core.Transaction) bool {
	sqlx, args := sqlbuilder.NewInsertBuilder().
		InsertInto(store.Schema().Table("transactions")).
		Cols("id", "timestamp", "hash", "reference").
		Values(tx.ID, tx.Timestamp.Format(time.RFC3339), "xyz", tx.Reference).
		BuildWithFlavor(store.Schema().Flavor())
	_, err := store.Schema().ExecContext(context.Background(), sqlx, args...)
	if !assert.NoError(t, err) {
		return false
	}

	for _, p := range tx.Postings {
		sqlx, args = sqlbuilder.NewInsertBuilder().
			InsertInto(store.Schema().Table("postings")).
			Cols("txid", "amount", "asset", "destination", "source").
			Values(tx.ID, p.Amount, p.Asset, p.Destination, p.Source).
			BuildWithFlavor(store.Schema().Flavor())
		_, err = store.Schema().ExecContext(context.Background(), sqlx, args...)
		if !assert.NoError(t, err) {
			return false
		}
	}

	for key, value := range tx.Metadata {
		if !v0AddMetadata(t, store, "transaction", fmt.Sprint(tx.ID), tx.Timestamp, key, value) {
			return false
		}
	}

	return true
}

func v0CountMetadata(t *testing.T, store *sqlstorage.Store) (int, bool) {
	sqlx, args := sqlbuilder.Select("count(*)").From(store.Schema().Table("metadata")).BuildWithFlavor(store.Schema().Flavor())
	rows, err := store.Schema().QueryContext(context.Background(), sqlx, args...)
	if !assert.NoError(t, err) {
		return 0, false
	}
	if !assert.True(t, rows.Next()) {
		return 0, false
	}
	count := 0
	err = rows.Scan(&count)
	if !assert.NoError(t, err) {
		return 0, false
	}
	defer func(rows *sql.Rows) {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}(rows)
	return count, true
}

func v0AddMetadata(t *testing.T, store *sqlstorage.Store, targetType, targetId string, timestamp time.Time, key string, value json.RawMessage) bool {
	count, ok := v0CountMetadata(t, store)
	if !ok {
		return false
	}

	sqlx, args := sqlbuilder.NewInsertBuilder().
		InsertInto(store.Schema().Table("metadata")).
		Cols("meta_id", "meta_target_type", "meta_target_id", "meta_key", "meta_value", "timestamp").
		Values(count+1, targetType, targetId, key, string(value), timestamp.Format(time.RFC3339)).
		BuildWithFlavor(store.Schema().Flavor())
	_, err := store.Schema().ExecContext(context.Background(), sqlx, args...)
	return assert.NoError(t, err)
}

var now = time.Now().Truncate(time.Second).UTC()

var postMigrate = map[string]func(t *testing.T, store *sqlstorage.Store){
	"0": func(t *testing.T, store *sqlstorage.Store) {
		tx1 := core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "world",
						Destination: "player1",
						Amount:      100,
						Asset:       "USD",
					},
					{
						Source:      "world",
						Destination: "player2",
						Amount:      100,
						Asset:       "USD",
					},
				},
				Metadata: core.Metadata{
					"players": json.RawMessage(`["player1", "player2"]`),
				},
				Reference: "tx1",
				Timestamp: now,
			},
			ID: 0,
		}
		if !v0CreateTransaction(t, store, tx1) {
			return
		}

		if !v0AddMetadata(t, store, "transaction", fmt.Sprint(tx1.ID), tx1.Timestamp, "info", json.RawMessage(`"Init game"`)) {
			return
		}

		if !v0AddMetadata(t, store, "transaction", fmt.Sprint(tx1.ID), tx1.Timestamp, "startedAt", json.RawMessage(tx1.Timestamp.Format(time.RFC3339))) {
			return
		} // Invalid json.RawMessage, it is to prevent failures when upgrading corrupted data
		if !v0AddMetadata(t, store, "account", "player1", now.Add(time.Second), "role", json.RawMessage(`"admin"`)) {
			return
		}
		if !v0AddMetadata(t, store, "account", "player1", now.Add(time.Second), "phones", json.RawMessage(`["0836656565"]`)) {
			return
		}

		tx2 := core.Transaction{
			TransactionData: core.TransactionData{
				Postings: []core.Posting{
					{
						Source:      "player1",
						Destination: "shop",
						Amount:      1,
						Asset:       "USD",
					},
				},
				Reference: "tx2",
				Timestamp: now.Add(2 * time.Second),
			},
			ID: 1,
		}
		if !v0CreateTransaction(t, store, tx2) {
			return
		}
	},
	"10": func(t *testing.T, store *sqlstorage.Store) {

		count, err := store.CountTransactions(context.Background(), storage.TransactionsQuery{})
		if !assert.NoError(t, err) {
			return
		}
		assert.EqualValues(t, 2, count)

		tx, err := store.GetTransaction(context.Background(), 0)
		if !assert.NoError(t, err) {
			return
		}

		if !assert.ElementsMatch(t, []core.Posting{
			{
				Source:      "world",
				Destination: "player1",
				Amount:      100,
				Asset:       "USD",
			},
			{
				Source:      "world",
				Destination: "player2",
				Amount:      100,
				Asset:       "USD",
			},
		}, tx.Postings) {
			return
		}
		tx.Postings = core.Postings{}

		if !assert.True(t, tx.Metadata.IsEquivalentTo(core.Metadata{
			"info":      json.RawMessage("\"Init game\""),
			"players":   json.RawMessage(`["player1", "player2"]`),
			"startedAt": json.RawMessage(fmt.Sprintf(`"%s"`, now.Format(time.RFC3339))),
		})) {
			return
		}
		tx.Metadata = core.Metadata{}

		assert.EqualValues(t, core.Transaction{
			TransactionData: core.TransactionData{
				Postings:  core.Postings{},
				Reference: "tx1",
				Metadata:  core.Metadata{},
				Timestamp: now,
			},
			PreCommitVolumes:  core.AccountsAssetsVolumes{},
			PostCommitVolumes: core.AccountsAssetsVolumes{},
		}, *tx)

		account, err := store.GetAccount(context.Background(), "player1")
		if !assert.NoError(t, err) {
			return
		}

		if !assert.Equal(t, core.Account{
			Address: "player1",
			Metadata: core.Metadata{
				"phones": json.RawMessage(`["0836656565"]`),
				"role":   json.RawMessage(`"admin"`),
			},
		}, *account) {
			return
		}

		volumes, err := store.GetAssetsVolumes(context.Background(), "player1")
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Equal(t, core.AssetsVolumes{
			"USD": {
				Input:  100,
				Output: 1,
			},
		}, volumes) {
			return
		}

		txs, err := store.GetTransactions(context.Background(), storage.TransactionsQuery{
			PageSize: 100,
		})
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Len(t, txs.Data, 2) {
			return
		}
		if !assert.EqualValues(t, "tx2", txs.Data[0].Reference) {
			return
		}
		if !assert.EqualValues(t, "tx1", txs.Data[1].Reference) {
			return
		}

		logs, err := store.Logs(context.Background())
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Len(t, logs, 7) {
			return
		}

		err = store.UpdateTransactionMetadata(context.Background(), 0, core.Metadata{
			"after": json.RawMessage("\"migrate\""),
		}, now.Add(2*time.Second))
		require.NoError(t, err)

		logs, err = store.Logs(context.Background())
		require.NoError(t, err)
		require.Len(t, logs, 8)

		index, ok := core.CheckHash(logs...)
		if !assert.Truef(t, ok, "error checking hash at index %d", index) {
			return
		}

		lastLog, err := store.LastLog(context.Background())
		require.NoError(t, err)

		expectedLogs := []core.Log{
			*lastLog,
			{
				ID:   6,
				Type: core.NewTransactionType,
				Data: core.Transaction{
					TransactionData: core.TransactionData{
						Postings: core.Postings{
							{
								Source:      "player1",
								Destination: "shop",
								Amount:      1,
								Asset:       "USD",
							},
						},
						Metadata:  core.Metadata{},
						Reference: "tx2",
						Timestamp: now.Add(2 * time.Second),
					},
					ID: 1,
				},
				Date: now.Add(2 * time.Second),
			},
			{
				ID:   5,
				Type: core.SetMetadataType,
				Data: core.SetMetadata{
					TargetType: "ACCOUNT",
					TargetID:   "player1",
					Metadata: core.Metadata{
						"phones": json.RawMessage(`["0836656565"]`),
					},
				},
				Date: now.Add(time.Second),
			},
			{
				ID:   4,
				Type: core.SetMetadataType,
				Data: core.SetMetadata{
					TargetType: "ACCOUNT",
					TargetID:   "player1",
					Metadata: core.Metadata{
						"role": json.RawMessage(`"admin"`),
					},
				},
				Date: now.Add(time.Second),
			},
			{
				ID:   3,
				Type: core.SetMetadataType,
				Data: core.SetMetadata{
					TargetType: "TRANSACTION",
					TargetID:   uint64(0),
					Metadata: core.Metadata{
						"startedAt": json.RawMessage(fmt.Sprintf(`"%s"`, now.Format(time.RFC3339))),
					},
				},
				Date: now,
			},
			{
				ID:   2,
				Type: core.SetMetadataType,
				Data: core.SetMetadata{
					TargetType: "TRANSACTION",
					TargetID:   uint64(0),
					Metadata: core.Metadata{
						"info": json.RawMessage(`"Init game"`),
					},
				},
				Date: now,
			},
			{
				ID:   1,
				Type: core.SetMetadataType,
				Data: core.SetMetadata{
					TargetType: "TRANSACTION",
					TargetID:   uint64(0),
					Metadata: core.Metadata{
						"players": json.RawMessage(`["player1", "player2"]`),
					},
				},
				Date: now,
			},
			{
				ID:   0,
				Type: core.NewTransactionType,
				Data: core.Transaction{
					TransactionData: core.TransactionData{
						Postings: core.Postings{
							{
								Source:      "world",
								Destination: "player1",
								Amount:      100,
								Asset:       "USD",
							},
							{
								Source:      "world",
								Destination: "player2",
								Amount:      100,
								Asset:       "USD",
							},
						},
						Metadata:  core.Metadata{},
						Reference: "tx1",
						Timestamp: now,
					},
				},
				Date: now,
			},
		}

		for i := len(expectedLogs) - 1; i >= 0; i-- {
			var previousLog *core.Log
			if i < len(expectedLogs)-1 {
				previousLog = &expectedLogs[i+1]
			}
			logs[i].Date = logs[i].Date.UTC()
			expectedLogs[i].Hash = ""
			expectedLogs[i].Hash = core.Hash(previousLog, expectedLogs[i])
			require.EqualValuesf(t, expectedLogs[i], logs[i], "Hash %d does not match", i)
		}
	},
}

func TestAllMigrations(t *testing.T) {

	if testing.Verbose() {
		l := logrus.New()
		l.SetLevel(logrus.DebugLevel)
		sharedlogging.SetFactory(sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l)))
	}

	driver, closeFunc, err := ledgertesting.StorageDriver()
	require.NoError(t, err)
	defer closeFunc()

	err = driver.Initialize(context.Background())
	require.NoError(t, err)

	s, _, err := driver.GetStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)
	store := s.(*sqlstorage.Store)

	migrations, err := sqlstorage.CollectMigrationFiles(sqlstorage.MigrationsFS)
	require.NoError(t, err)

	for _, migration := range migrations {

		modified, err := sqlstorage.Migrate(context.Background(), store.Schema(), migration)
		require.NoError(t, err)
		require.True(t, modified)

		pm := postMigrate[migration.Number]
		if pm != nil {
			pm(t, store)
		}
	}
}
