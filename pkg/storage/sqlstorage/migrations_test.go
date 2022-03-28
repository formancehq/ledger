package sqlstorage_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger/query"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage/sqlstorage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"os"
	"strings"
	"testing"
	"testing/fstest"
	"time"
)

var v0CreateTransaction = func(t *testing.T, store *sqlstorage.Store, tx core.Transaction) bool {
	sqlx, args := sqlbuilder.NewInsertBuilder().
		InsertInto(store.Schema().Table("transactions")).
		Cols("id", "timestamp", "hash", "reference").
		Values(tx.ID, tx.Timestamp, "xyz", tx.Reference).
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
	defer rows.Close()
	return count, true
}

func v0AddMetadata(t *testing.T, store *sqlstorage.Store, targetType, targetId, timestamp, key string, value json.RawMessage) bool {
	count, ok := v0CountMetadata(t, store)
	if !ok {
		return false
	}

	sqlx, args := sqlbuilder.NewInsertBuilder().
		InsertInto(store.Schema().Table("metadata")).
		Cols("meta_id", "meta_target_type", "meta_target_id", "meta_key", "meta_value", "timestamp").
		Values(count+1, targetType, targetId, key, string(value), timestamp).
		BuildWithFlavor(store.Schema().Flavor())
	_, err := store.Schema().ExecContext(context.Background(), sqlx, args...)
	if !assert.NoError(t, err) {
		return false
	}
	return true
}

/** Postgres and SQLite doesn't have the same behavior regardings json processing
Postgres will clean the json and keep a space after semicolons.
Sqlite will clean the json and minify it.
So we can't directy compare metadata.
compareMetadata convert metadata to map[string]interface{} which can be compared.
*/
func compareMetadata(t *testing.T, m1, m2 core.Metadata) bool {
	d1, err := json.Marshal(m1)
	if !assert.NoError(t, err) {
		return false
	}
	map1 := make(map[string]interface{})
	err = json.Unmarshal(d1, &map1)
	if !assert.NoError(t, err) {
		return false
	}

	d2, err := json.Marshal(m2)
	if !assert.NoError(t, err) {
		return false
	}
	map2 := make(map[string]interface{})
	err = json.Unmarshal(d2, &map2)
	if !assert.NoError(t, err) {
		return false
	}

	return assert.EqualValues(t, map1, map2)
}

var now = time.Now().Round(time.Second)

var postMigrate = map[string]func(t *testing.T, store *sqlstorage.Store){
	"0.sql": func(t *testing.T, store *sqlstorage.Store) {
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
			},
			ID:        0,
			Timestamp: now.Format(time.RFC3339),
		}
		if !v0CreateTransaction(t, store, tx1) {
			return
		}

		if !v0AddMetadata(t, store, "transaction", fmt.Sprint(tx1.ID), tx1.Timestamp, "info", json.RawMessage(`"Init game"`)) {
			return
		}

		if !v0AddMetadata(t, store, "transaction", fmt.Sprint(tx1.ID), tx1.Timestamp, "startedAt", json.RawMessage(tx1.Timestamp)) {
			return
		} // Invalid json.RawMessage, it is to prevent failures when upgrading corrupted data
		if !v0AddMetadata(t, store, "account", "player1", now.Add(time.Second).Format(time.RFC3339), "role", json.RawMessage(`"admin"`)) {
			return
		}
		if !v0AddMetadata(t, store, "account", "player1", now.Add(time.Second).Format(time.RFC3339), "phones", json.RawMessage(`["0836656565"]`)) {
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
			},
			ID:        1,
			Timestamp: now.Add(2 * time.Second).Format(time.RFC3339),
		}
		if !v0CreateTransaction(t, store, tx2) {
			return
		}
	},
	"1.sql": func(t *testing.T, store *sqlstorage.Store) {

		count, err := store.CountTransactions(context.Background())
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
		tx.Postings = nil

		if !assert.True(t, compareMetadata(t, core.Metadata{
			"info":      json.RawMessage("\"Init game\""),
			"players":   json.RawMessage(`["player1", "player2"]`),
			"startedAt": json.RawMessage(fmt.Sprintf(`"%s"`, now.Format(time.RFC3339))),
		}, tx.Metadata)) {
			return
		}
		tx.Metadata = nil

		if !assert.EqualValues(t, core.Transaction{
			TransactionData: core.TransactionData{
				Reference: "tx1",
			},
			Timestamp: now.Format(time.RFC3339),
		}, tx) {
			return
		}

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
		}, account) {
			return
		}

		volumes, err := store.AggregateVolumes(context.Background(), "player1")
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Equal(t, map[string]map[string]int64{
			"USD": {
				"input":  100,
				"output": 1,
			},
		}, volumes) {
			return
		}

		balances, err := store.AggregateBalances(context.Background(), "player1")
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Equal(t, map[string]int64{
			"USD": 99,
		}, balances) {
			return
		}

		txs, err := store.FindTransactions(context.Background(), query.Query{
			Limit: 100,
		})
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Len(t, txs.Data, 2) {
			return
		}
		if !assert.EqualValues(t, "tx2", txs.Data.([]core.Transaction)[0].Reference) {
			return
		}
		if !assert.EqualValues(t, "tx1", txs.Data.([]core.Transaction)[1].Reference) {
			return
		}

		logs, err := store.Logs(context.Background())
		if !assert.NoError(t, err) {
			return
		}
		if !assert.Len(t, logs, 7) {
			return
		}

		expectedLogs := []core.Log{
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
					},
					ID:        1,
					Timestamp: now.Add(2 * time.Second).Format(time.RFC3339),
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
					},
					Timestamp: now.Format(time.RFC3339),
				},
				Date: now,
			},
		}

		for i := 0; i < len(expectedLogs); i++ {
			if !assert.EqualValues(t, expectedLogs[i], logs[i]) {
				return
			}
		}

	},
}

func TestMigrates(t *testing.T) {

	if testing.Verbose() {
		l := logrus.New()
		l.SetLevel(logrus.DebugLevel)
		sharedlogging.SetFactory(sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l)))
	}

	driver, close, err := ledgertesting.Driver()
	if !assert.NoError(t, err) {
		return
	}
	defer close()

	err = driver.Initialize(context.Background())
	if !assert.NoError(t, err) {
		return
	}

	s, _, err := driver.GetStore(context.Background(), uuid.New(), true)
	if !assert.NoError(t, err) {
		return
	}
	store := s.(*sqlstorage.Store)
	flavor := strings.ToLower(store.Schema().Flavor().String())

	migrationsFs := fstest.MapFS{}
	sqlstorage.MigrationsFs = migrationsFs

	entries, err := os.ReadDir("./migrations/" + flavor)
	if !assert.NoError(t, err) {
		return
	}

	for _, entry := range entries {
		file := fmt.Sprintf("migrations/%s/%s", flavor, entry.Name())
		data, err := os.ReadFile(file)
		if !assert.NoError(t, err) {
			return
		}
		migrationsFs[file] = &fstest.MapFile{
			Data: data,
		}

		modified, err := store.Initialize(context.Background())
		if !assert.NoError(t, err) {
			return
		}
		if !assert.True(t, modified) {
			return
		}
		pm := postMigrate[entry.Name()]
		if pm != nil {
			pm(t, store)
		}
	}
}
