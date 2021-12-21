package sqlstorage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/huandu/go-sqlbuilder"
	_ "github.com/jackc/pgx/v4/stdlib"
	_ "github.com/mattn/go-sqlite3"
	"github.com/numary/ledger/core"
	"github.com/numary/ledger/ledger/query"
	"github.com/ory/dockertest/v3"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func TestStore(t *testing.T) {

	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	pool, err := dockertest.NewPool("")
	assert.NoError(t, err)

	resource, err := pool.Run("postgres", "13-alpine", []string{
		"POSTGRES_USER=root",
		"POSTGRES_PASSWORD=root",
		"POSTGRES_DB=ledger",
	})
	assert.NoError(t, err)

	defer func() {
		err := pool.Purge(resource)
		assert.NoError(t, err)
	}()

	type testCase struct {
		driver     string
		connString string
		flavor     sqlbuilder.Flavor
	}

	for _, testCase := range []testCase{
		{
			driver:     "sqlite3",
			connString: "file::memory:?cache=shared",
			flavor:     sqlbuilder.SQLite,
		},
		{
			driver:     "pgx",
			connString: "postgresql://root:root@localhost:" + resource.GetPort("5432/tcp") + "/ledger",
			flavor:     sqlbuilder.PostgreSQL,
		},
	} {
		t.Run(testCase.driver, func(t *testing.T) {
			db, err := sql.Open(testCase.driver, testCase.connString)
			assert.NoError(t, err)
			defer db.Close()

			counter := 0
			for {
				err = db.Ping()
				if err != nil {
					if counter < 5 {
						counter++
						<-time.After(time.Second)
						continue
					}
					assert.Fail(t, "timeout waiting database: %s", err)
					return
				}
				break
			}

			store, err := NewStore("ledger", testCase.flavor, db, func(ctx context.Context) error {
				return db.Close()
			})
			assert.NoError(t, err)

			err = store.Initialize(context.Background())
			assert.NoError(t, err)

			txs := []core.Transaction{
				{
					ID: 1,
					Postings: []core.Posting{
						{
							Source:      "world",
							Destination: "central_bank",
							Amount:      100,
							Asset:       "USD",
						},
					},
					Metadata: map[string]json.RawMessage{
						"lastname": json.RawMessage(`"XXX"`),
					},
					Timestamp: time.Now().Format(time.RFC3339),
				},
			}
			err = store.SaveTransactions(context.Background(), txs)
			assert.NoError(t, err)

			err = store.SaveMeta(context.Background(), 1, time.Now().Format(time.RFC3339),
				"transaction", fmt.Sprint(txs[0].ID), "firstname", "\"YYY\"")
			assert.NoError(t, err)

			lastTx, err := store.LastTransaction(context.Background())
			assert.NoError(t, err)
			assert.NotNil(t, lastTx)
			assert.Equal(t, core.Transaction{
				ID:       txs[0].ID,
				Postings: txs[0].Postings,
				Metadata: core.Metadata{
					"lastname":  json.RawMessage(`"XXX"`),
					"firstname": json.RawMessage(`"YYY"`),
				},
				Timestamp: txs[0].Timestamp,
			}, *lastTx)

			lastMetaId, err := store.LastMetaID(context.Background())
			assert.NoError(t, err)
			assert.EqualValues(t, 1, lastMetaId)

			countAccounts, err := store.CountAccounts(context.Background())
			assert.EqualValues(t, 2, countAccounts) // world + central_bank

			balances, err := store.AggregateBalances(context.Background(), "central_bank")
			assert.NoError(t, err)
			assert.Len(t, balances, 1)
			assert.EqualValues(t, 100, balances["USD"])

			volumes, err := store.AggregateVolumes(context.Background(), "central_bank")
			assert.NoError(t, err)
			assert.Len(t, volumes, 1)
			assert.Len(t, volumes["USD"], 1)
			assert.EqualValues(t, 100, volumes["USD"]["input"])

			countMeta, err := store.CountMeta(context.Background())
			assert.NoError(t, err)
			assert.EqualValues(t, 2, countMeta)

			countTransactions, err := store.CountTransactions(context.Background())
			assert.NoError(t, err)
			assert.EqualValues(t, 1, countTransactions)

			accounts, err := store.FindAccounts(context.Background(), query.Query{})
			assert.NoError(t, err)
			assert.EqualValues(t, 2, accounts.Total)
		})
	}
}
