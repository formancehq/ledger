package add_pre_post_volumes_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
	ledgerstore "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger"
	add_pre_post_volumes "github.com/formancehq/ledger/pkg/storage/sqlstorage/ledger/migrates/9-add-pre-post-volumes"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/migrations"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/schema"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/sqlstoragetesting"
	"github.com/formancehq/stack/libs/go-libs/pgtesting"
	"github.com/pborman/uuid"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
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
				Amount:      big.NewInt(100),
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
			"bank": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(100),
				},
			},
			"bank": {
				"USD": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		},
	},
	{
		postings: core.Postings{
			{
				Source:      "world",
				Destination: "bank2",
				Amount:      big.NewInt(100),
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(100),
				},
			},
			"bank2": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(200),
				},
			},
			"bank2": {
				"USD": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		},
	},
	{
		postings: core.Postings{
			{
				Source:      "world",
				Destination: "bank",
				Amount:      big.NewInt(100),
				Asset:       "USD",
			},
			{
				Source:      "world",
				Destination: "bank2",
				Amount:      big.NewInt(100),
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(200),
				},
			},
			"bank": {
				"USD": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
			"bank2": {
				"USD": {
					Input:  big.NewInt(100),
					Output: big.NewInt(0),
				},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"world": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(400),
				},
			},
			"bank2": {
				"USD": {
					Input:  big.NewInt(200),
					Output: big.NewInt(0),
				},
			},
			"bank": {
				"USD": {
					Input:  big.NewInt(200),
					Output: big.NewInt(0),
				},
			},
		},
	},
	{
		postings: core.Postings{
			{
				Source:      "bank",
				Destination: "user:1",
				Amount:      big.NewInt(10),
				Asset:       "USD",
			},
			{
				Source:      "bank",
				Destination: "user:2",
				Amount:      big.NewInt(90),
				Asset:       "USD",
			},
		},
		expectedPreCommitVolumes: core.AccountsAssetsVolumes{
			"bank": {
				"USD": {
					Input:  big.NewInt(200),
					Output: big.NewInt(0),
				},
			},
			"user:1": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
			"user:2": {
				"USD": {
					Input:  big.NewInt(0),
					Output: big.NewInt(0),
				},
			},
		},
		expectedPostCommitVolumes: core.AccountsAssetsVolumes{
			"bank": {
				"USD": {
					Input:  big.NewInt(200),
					Output: big.NewInt(100),
				},
			},
			"user:1": {
				"USD": {
					Input:  big.NewInt(10),
					Output: big.NewInt(0),
				},
			},
			"user:2": {
				"USD": {
					Input:  big.NewInt(90),
					Output: big.NewInt(0),
				},
			},
		},
	},
}

func TestMigrate9(t *testing.T) {
	require.NoError(t, pgtesting.CreatePostgresServer())
	defer func() {
		require.NoError(t, pgtesting.DestroyPostgresServer())
	}()

	driver := sqlstoragetesting.StorageDriver(t)

	require.NoError(t, driver.Initialize(context.Background()))
	store, _, err := driver.GetLedgerStore(context.Background(), uuid.New(), true)
	require.NoError(t, err)

	schema := store.(*ledgerstore.Store).Schema()

	ms, err := migrations.CollectMigrationFiles(ledgerstore.MigrationsFS)
	require.NoError(t, err)

	modified, err := migrations.Migrate(context.Background(), schema, ms[0:9]...)
	require.NoError(t, err)
	require.True(t, modified)

	now := core.Now()
	for i, tc := range testCases {
		txData, err := json.Marshal(struct {
			add_pre_post_volumes.Transaction
			Date core.Time `json:"timestamp"`
		}{
			Transaction: add_pre_post_volumes.Transaction{
				ID:       uint64(i),
				Postings: tc.postings,
			},
			Date: now,
		})
		require.NoError(t, err)

		l := &add_pre_post_volumes.Log{
			ID:   uint64(i),
			Data: txData,
			Type: core.NewTransactionLogType.String(),
			Date: now,
		}

		_, err = schema.NewInsert("log").
			Model(l).
			Column("id", "data", "type", "date").
			Exec(context.Background())

		require.NoError(t, err)
	}

	transactionQuery := storage.NewTransactionsQuery()
	sb := buildTransactionsQuery(context.Background(), schema, transactionQuery)
	count, err := sb.Count(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(count), uint64(len(testCases)))

	sqlTx, err := schema.BeginTx(context.Background(), &sql.TxOptions{})
	require.NoError(t, err)

	require.NoError(t, add_pre_post_volumes.Upgrade(context.Background(), schema, sqlTx))
	require.NoError(t, sqlTx.Commit())

	for i, tc := range testCases {
		sb := schema.NewSelect(ledgerstore.TransactionsTableName).
			Model((*ledgerstore.Transaction)(nil)).
			Column("pre_commit_volumes", "post_commit_volumes").
			Where("id = ?", i)
		row := schema.QueryRowContext(context.Background(), sb.String())
		require.NoError(t, row.Err())

		preCommitVolumes, postCommitVolumes := core.AccountsAssetsVolumes{}, core.AccountsAssetsVolumes{}
		require.NoError(t, row.Scan(&preCommitVolumes, &postCommitVolumes))

		require.Equal(t, tc.expectedPreCommitVolumes, preCommitVolumes)
		require.Equal(t, tc.expectedPostCommitVolumes, postCommitVolumes)
	}

}

type Transactions struct {
	bun.BaseModel `bun:"transactions,alias:transactions"`

	ID                uint64          `bun:"id,type:bigint,unique"`
	Timestamp         core.Time       `bun:"timestamp,type:timestamptz"`
	Reference         string          `bun:"reference,type:varchar,unique,nullzero"`
	Hash              string          `bun:"hash,type:varchar"`
	Postings          json.RawMessage `bun:"postings,type:jsonb"`
	Metadata          json.RawMessage `bun:"metadata,type:jsonb,default:'{}'"`
	PreCommitVolumes  json.RawMessage `bun:"pre_commit_volumes,type:jsonb"`
	PostCommitVolumes json.RawMessage `bun:"post_commit_volumes,type:jsonb"`
}

var addressQueryRegexp = regexp.MustCompile(`^(\w+|\*|\.\*)(:(\w+|\*|\.\*))*$`)

func buildTransactionsQuery(ctx context.Context, schema schema.Schema, p storage.TransactionsQuery) *bun.SelectQuery {
	sb := schema.NewSelect("transactions").Model((*Transactions)(nil))

	var (
		destination = p.Filters.Destination
		source      = p.Filters.Source
		account     = p.Filters.Account
		reference   = p.Filters.Reference
		startTime   = p.Filters.StartTime
		endTime     = p.Filters.EndTime
		metadata    = p.Filters.Metadata
	)

	sb.Column("id", "timestamp", "reference", "metadata", "postings", "pre_commit_volumes", "post_commit_volumes").
		Distinct()
	if source != "" || destination != "" || account != "" {
		// new wildcard handling
		sb.Join(fmt.Sprintf(
			"JOIN %s postings",
			schema.Table("postings"),
		)).JoinOn("postings.txid = transactions.id")
	}
	if source != "" {
		if !addressQueryRegexp.MatchString(source) {
			// deprecated regex handling
			sb.Where(fmt.Sprintf("%s(postings, ?)", schema.Table("use_account_as_source")), source)
		} else {
			// new wildcard handling
			src := strings.Split(source, ":")
			sb.Where(fmt.Sprintf("jsonb_array_length(postings.source) = %d", len(src)))

			for i, segment := range src {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				sb.Where(fmt.Sprintf("postings.source @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
			}
		}
	}
	if destination != "" {
		if !addressQueryRegexp.MatchString(destination) {
			// deprecated regex handling
			sb.Where(fmt.Sprintf("%s(postings, ?)", schema.Table("use_account_as_destination")), destination)
		} else {
			// new wildcard handling
			dst := strings.Split(destination, ":")
			sb.Where(fmt.Sprintf("jsonb_array_length(postings.destination) = %d", len(dst)))
			for i, segment := range dst {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				sb.Where(fmt.Sprintf("postings.destination @@ ('$[%d] == \"' || ?::text || '\"')::jsonpath", i), segment)
			}
		}
	}
	if account != "" {
		if !addressQueryRegexp.MatchString(account) {
			// deprecated regex handling
			sb.Where(fmt.Sprintf("%s(postings, ?)", schema.Table("use_account")), account)
		} else {
			// new wildcard handling
			dst := strings.Split(account, ":")
			sb.Where(fmt.Sprintf("(jsonb_array_length(postings.destination) = %d OR jsonb_array_length(postings.source) = %d)", len(dst), len(dst)))
			for i, segment := range dst {
				if segment == ".*" || segment == "*" || segment == "" {
					continue
				}

				sb.Where(fmt.Sprintf("(postings.source @@ ('$[%d] == \"' || ?0::text || '\"')::jsonpath OR postings.destination @@ ('$[%d] == \"' || ?0::text || '\"')::jsonpath)", i, i), segment)
			}
		}
	}
	if reference != "" {
		sb.Where("reference = ?", reference)
	}
	if !startTime.IsZero() {
		sb.Where("timestamp >= ?", startTime.UTC())
	}
	if !endTime.IsZero() {
		sb.Where("timestamp < ?", endTime.UTC())
	}

	for key, value := range metadata {
		sb.Where(schema.Table(
			fmt.Sprintf("%s(metadata, ?, '%s')",
				ledgerstore.SQLCustomFuncMetaCompare, strings.ReplaceAll(key, ".", "', '")),
		), value)
	}

	return sb
}
