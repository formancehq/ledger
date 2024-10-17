//go:build it

package bucket_test

import (
	"context"
	"github.com/formancehq/go-libs/v2/testing/migrations"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/formancehq/ledger/internal/storage/driver"
	"math/big"
	"testing"

	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/bun/bundebug"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

func TestMigrations(t *testing.T) {
	t.Parallel()
	ctx := logging.TestingContext()

	pgServer := srv.NewDatabase(t)

	hooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		hooks = append(hooks, bundebug.NewQueryHook())
	}

	db, err := bunconnect.OpenSQLDB(ctx, pgServer.ConnectionOptions(), hooks...)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, db.Close())
	})

	require.NoError(t, driver.Migrate(ctx, db))

	test := migrations.NewMigrationTest(t, bucket.GetMigrator(ledger.DefaultBucket), db)
	test.Append(12, removeSequenceOnMovesTable)
	test.Append(16, changePVCColumnTypeOfMoves)
	test.Append(18, addTransactionsPVC)
	test.Append(21, addAccountsVolumesTable)
	test.Append(22, addTransactionIDOnTransactionsMetadataTable)
	test.Append(23, addAccountAddressOnAccountsMetadataTable)
	test.Run()
}

var (
	now                        = time.Now()
	removeSequenceOnMovesTable = migrations.Hook{
		Before: func(ctx context.Context, t *testing.T, db bun.IDB) {
			// insert some accounts
			_, err := db.NewInsert().
				Model(&map[string]any{
					"ledger":         "foo",
					"address":        "world",
					"address_array":  []string{"world"},
					"seq":            1,
					"insertion_date": now,
					"updated_at":     now,
				}).
				TableExpr(ledger.DefaultBucket + ".accounts").
				Exec(ctx)
			require.NoError(t, err)

			_, err = db.NewInsert().
				Model(&map[string]any{
					"ledger":         "foo",
					"address":        "bank",
					"address_array":  []string{"bank"},
					"seq":            2,
					"insertion_date": now,
					"updated_at":     now,
				}).
				TableExpr(ledger.DefaultBucket + ".accounts").
				Exec(ctx)
			require.NoError(t, err)

			// insert a transaction
			_, err = db.NewInsert().
				Model(&map[string]any{
					"ledger":       "foo",
					"id":           1,
					"seq":          1,
					"timestamp":    time.Now(),
					"postings":     []any{},
					"sources":      []string{"world"},
					"destinations": []string{"bank"},
				}).
				TableExpr(ledger.DefaultBucket + ".transactions").
				Exec(ctx)
			require.NoError(t, err)

			// insert moves
			_, err = db.NewInsert().
				Model(&map[string]any{
					"ledger":                        "foo",
					"seq":                           1,
					"asset":                         "USD",
					"amount":                        big.NewInt(100),
					"transactions_seq":              1,
					"accounts_seq":                  1,
					"account_address":               "world",
					"account_address_array":         []string{"world"},
					"post_commit_volumes":           "(0, 100)",
					"post_commit_effective_volumes": "(0, 100)",
					"insertion_date":                now,
					"effective_date":                now,
					"is_source":                     true,
				}).
				TableExpr(ledger.DefaultBucket + ".moves").
				Exec(ctx)
			require.NoError(t, err)

			_, err = db.NewInsert().
				Model(&map[string]any{
					"ledger":                        "foo",
					"seq":                           3,
					"asset":                         "USD",
					"amount":                        big.NewInt(100),
					"transactions_seq":              1,
					"accounts_seq":                  2,
					"account_address":               "bank",
					"account_address_array":         []string{"bank"},
					"post_commit_volumes":           "(100, 0)",
					"post_commit_effective_volumes": "(100, 0)",
					"insertion_date":                now,
					"effective_date":                now,
					"is_source":                     false,
				}).
				TableExpr(ledger.DefaultBucket + ".moves").
				Exec(ctx)
			require.NoError(t, err)
		},
		After: func(ctx context.Context, t *testing.T, db bun.IDB) {
			ret := make([]map[string]any, 0)
			err := db.NewSelect().
				ModelTableExpr(ledger.DefaultBucket + ".moves").
				Model(&ret).
				Scan(ctx)
			require.NoError(t, err)
			require.Len(t, ret, 2)
			require.Equal(t, int64(1), ret[0]["transactions_id"])
			require.Equal(t, int64(1), ret[1]["transactions_id"])
		},
	}
	changePVCColumnTypeOfMoves = migrations.Hook{
		After: func(ctx context.Context, t *testing.T, db bun.IDB) {
			type model struct {
				bun.BaseModel `bun:"alias:moves"`

				Volumes          ledger.Volumes `bun:"post_commit_volumes"`
				EffectiveVolumes ledger.Volumes `bun:"post_commit_effective_volumes"`
			}
			ret := make([]model, 0)
			err := db.NewSelect().
				Model(&ret).
				ModelTableExpr(ledger.DefaultBucket + ".moves").
				Order("seq").
				Scan(ctx)
			require.NoError(t, err)

			require.Len(t, ret, 2)
			require.Equal(t, ledger.NewVolumesInt64(0, 100), ret[0].Volumes)
			require.Equal(t, ledger.NewVolumesInt64(100, 0), ret[1].Volumes)
			require.Equal(t, ledger.NewVolumesInt64(0, 100), ret[0].EffectiveVolumes)
			require.Equal(t, ledger.NewVolumesInt64(100, 0), ret[1].EffectiveVolumes)
		},
	}
	addTransactionsPVC = migrations.Hook{
		After: func(ctx context.Context, t *testing.T, db bun.IDB) {
			type model struct {
				bun.BaseModel `bun:"alias:transactions"`

				PostCommitVolumes ledger.PostCommitVolumes `bun:"post_commit_volumes"`
			}
			ret := make([]model, 0)
			err := db.NewSelect().
				Model(&ret).
				ModelTableExpr(ledger.DefaultBucket + ".transactions").
				Order("seq").
				Scan(ctx)
			require.NoError(t, err)

			require.Len(t, ret, 1)
			require.Equal(t, ledger.PostCommitVolumes{
				"world": {
					"USD": ledger.NewVolumesInt64(0, 100),
				},
				"bank": {
					"USD": ledger.NewVolumesInt64(100, 0),
				},
			}, ret[0].PostCommitVolumes)
		},
	}
	addAccountsVolumesTable = migrations.Hook{
		After: func(ctx context.Context, t *testing.T, db bun.IDB) {
			type model struct {
				bun.BaseModel `bun:"alias:accounts_volumes"`

				Address string   `bun:"accounts_address"`
				Asset   string   `bun:"asset"`
				Input   *big.Int `bun:"input"`
				Output  *big.Int `bun:"output"`
			}
			ret := make([]model, 0)
			err := db.NewSelect().
				Model(&ret).
				ModelTableExpr(ledger.DefaultBucket + ".accounts_volumes").
				Order("accounts_address").
				Scan(ctx)
			require.NoError(t, err)

			require.Len(t, ret, 2)
			require.Equal(t, model{
				Address: "bank",
				Asset:   "USD",
				Input:   big.NewInt(100),
				Output:  big.NewInt(0),
			}, ret[0])
			require.Equal(t, model{
				Address: "world",
				Asset:   "USD",
				Input:   big.NewInt(0),
				Output:  big.NewInt(100),
			}, ret[1])
		},
	}
	addTransactionIDOnTransactionsMetadataTable = migrations.Hook{
		Before: func(ctx context.Context, t *testing.T, db bun.IDB) {
			_, err := db.NewInsert().
				Model(&map[string]any{
					"ledger":           "foo",
					"transactions_seq": 1,
					"revision":         1,
					"date":             now,
					"metadata":         map[string]string{"foo": "bar"},
				}).
				TableExpr(ledger.DefaultBucket + ".transactions_metadata").
				Exec(ctx)
			require.NoError(t, err)
		},
		After: func(ctx context.Context, t *testing.T, db bun.IDB) {
			type model struct {
				bun.BaseModel `bun:"alias:transactions_metadata"`

				TransactionID int `bun:"transactions_id"`
			}
			ret := make([]model, 0)
			err := db.NewSelect().
				Model(&ret).
				ModelTableExpr(ledger.DefaultBucket + ".transactions_metadata").
				Scan(ctx)
			require.NoError(t, err)
			require.Len(t, ret, 1)
			require.Equal(t, 1, ret[0].TransactionID)
		},
	}
	addAccountAddressOnAccountsMetadataTable = migrations.Hook{
		Before: func(ctx context.Context, t *testing.T, db bun.IDB) {
			_, err := db.NewInsert().
				Model(&map[string]any{
					"ledger":       "foo",
					"accounts_seq": 1,
					"revision":     1,
					"date":         now,
					"metadata":     map[string]string{"foo": "bar"},
				}).
				TableExpr(ledger.DefaultBucket + ".accounts_metadata").
				Exec(ctx)
			require.NoError(t, err)
		},
		After: func(ctx context.Context, t *testing.T, db bun.IDB) {
			type model struct {
				bun.BaseModel `bun:"alias:accounts_metadata"`

				Address string `bun:"accounts_address"`
			}
			ret := make([]model, 0)
			err := db.NewSelect().
				Model(&ret).
				ModelTableExpr(ledger.DefaultBucket + ".accounts_metadata").
				Scan(ctx)
			require.NoError(t, err)
			require.Len(t, ret, 1)
			require.Equal(t, "world", ret[0].Address)
		},
	}
)
