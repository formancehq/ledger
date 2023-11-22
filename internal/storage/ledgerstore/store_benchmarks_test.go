package ledgerstore

import (
	"context"
	"flag"
	"fmt"
	"math/big"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/stack/libs/go-libs/bun/bunexplain"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/metadata"
	"github.com/formancehq/stack/libs/go-libs/query"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

var nbTransactions = flag.Int("transactions", 10000, "number of transactions to create")

func BenchmarkList(b *testing.B) {
	const batchSize = 1000

	ctx := logging.TestingContext()
	hooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		hooks = append(hooks, bunexplain.NewExplainHook())
	}
	store := newLedgerStore(b, hooks...)

	var lastLog *ledger.ChainedLog
	for i := 0; i < *nbTransactions/batchSize; i++ {
		batch := make([]*ledger.ChainedLog, 0)
		appendLog := func(log *ledger.Log) {
			chainedLog := log.ChainLog(lastLog)
			batch = append(batch, chainedLog)
			lastLog = chainedLog
		}
		for j := 0; j < batchSize; j += 2 {
			provision := big.NewInt(10000)
			itemPrice := provision.Div(provision, big.NewInt(2))
			fees := itemPrice.Div(itemPrice, big.NewInt(100)) // 1%

			appendLog(ledger.NewTransactionLog(
				ledger.NewTransaction().WithPostings(ledger.NewPosting(
					"world", fmt.Sprintf("player:%d", j/2), "USD/2", provision,
				)).WithID(big.NewInt(int64(i*batchSize+j))),
				map[string]metadata.Metadata{},
			))
			appendLog(ledger.NewTransactionLog(
				ledger.NewTransaction().WithPostings(
					ledger.NewPosting(fmt.Sprintf("player:%d", j/2), "seller", "USD/2", itemPrice),
					ledger.NewPosting("seller", "fees", "USD/2", fees),
				).WithID(big.NewInt(int64(i*batchSize+j+1))),
				map[string]metadata.Metadata{},
			))
			status := "pending"
			if j%8 == 0 {
				status = "terminated"
			}
			appendLog(ledger.NewSetMetadataLog(ledger.Now(), ledger.SetMetadataLogPayload{
				TargetType: ledger.MetaTargetTypeTransaction,
				TargetID:   big.NewInt(int64(i*batchSize + j + 1)),
				Metadata: map[string]string{
					"status": status,
				},
			}))
		}
		require.NoError(b, store.InsertLogs(ctx, batch...))
	}

	nbAccounts := batchSize / 2

	for i := 0; i < nbAccounts; i++ {
		lastLog = ledger.NewSetMetadataLog(ledger.Now(), ledger.SetMetadataLogPayload{
			TargetType: ledger.MetaTargetTypeAccount,
			TargetID:   fmt.Sprintf("player:%d", i),
			Metadata: map[string]string{
				"level": fmt.Sprint(i % 4),
			},
		}).ChainLog(lastLog)
		require.NoError(b, store.InsertLogs(ctx, lastLog))
	}

	benchmarksReadTransactions(b, ctx, store, *nbTransactions, nbAccounts)
	benchmarksReadAccounts(b, ctx, store, nbAccounts)
	benchmarksGetAggregatedBalances(b, ctx, store, nbAccounts)
}

func benchmarksReadTransactions(b *testing.B, ctx context.Context, store *Store, nbTransactions, nbAccounts int) {
	type testCase struct {
		name                   string
		query                  query.Builder
		allowEmptyResponse     bool
		expandVolumes          bool
		expandEffectiveVolumes bool
	}

	testCases := []testCase{
		{
			name: "with no query",
		},
		{
			name:  "using an exact address",
			query: query.Match("account", fmt.Sprintf("player:%d", nbAccounts-1)), // Last inserted account
		},
		{
			name:  "using an address segment",
			query: query.Match("account", fmt.Sprintf(":%d", nbAccounts-1)),
		},
		{
			name:  "using a metadata metadata",
			query: query.Match("metadata[status]", "terminated"),
		},
		{
			name:               "using non existent account by exact address",
			query:              query.Match("account", fmt.Sprintf("player:%d", nbAccounts)),
			allowEmptyResponse: true,
		},
		{
			name:               "using non existent metadata",
			query:              query.Match("metadata[foo]", "bar"),
			allowEmptyResponse: true,
		},
		{
			name:          "with expand volumes",
			expandVolumes: true,
		},
		{
			name:                   "with expand effective volumes",
			expandEffectiveVolumes: true,
		},
	}

	for _, t := range testCases {
		t := t
		b.Run("listing transactions "+t.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q := NewGetTransactionsQuery(PaginatedQueryOptions[PITFilterWithVolumes]{
					PageSize:     10,
					QueryBuilder: t.query,
				})
				if t.expandVolumes {
					q = q.WithExpandVolumes()
				}
				if t.expandEffectiveVolumes {
					q = q.WithExpandEffectiveVolumes()
				}
				ret, err := store.GetTransactions(ctx, q)
				require.NoError(b, err)
				if !t.allowEmptyResponse && len(ret.Data) == 0 {
					require.Fail(b, "response should not be empty")
				}
			}
		})
	}
}

func benchmarksReadAccounts(b *testing.B, ctx context.Context, store *Store, nbAccounts int) {
	type testCase struct {
		name                                  string
		query                                 query.Builder
		allowEmptyResponse                    bool
		expandVolumes, expandEffectiveVolumes bool
	}

	testCases := []testCase{
		{
			name: "with no query",
		},
		{
			name:  "filtering on address segment",
			query: query.Match("address", ":0"),
		},
		{
			name:  "filtering on metadata",
			query: query.Match("metadata[level]", "2"),
		},
		{
			name:          "with expand volumes",
			expandVolumes: true,
		},
		{
			name:                   "with expand effective volumes",
			expandEffectiveVolumes: true,
		},
	}

	for _, t := range testCases {
		t := t
		b.Run("listing accounts "+t.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				q := NewGetAccountsQuery(PaginatedQueryOptions[PITFilterWithVolumes]{
					PageSize:     10,
					QueryBuilder: t.query,
				})
				if t.expandVolumes {
					q = q.WithExpandVolumes()
				}
				if t.expandEffectiveVolumes {
					q = q.WithExpandEffectiveVolumes()
				}
				ret, err := store.GetAccountsWithVolumes(ctx, q)
				require.NoError(b, err)
				if !t.allowEmptyResponse && len(ret.Data) == 0 {
					require.Fail(b, "response should not be empty")
				}
			}
		})
	}
}

func benchmarksGetAggregatedBalances(b *testing.B, ctx context.Context, store *Store, nbAccounts int) {
	type testCase struct {
		name               string
		query              query.Builder
		allowEmptyResponse bool
	}

	testCases := []testCase{
		{
			name: "with no query",
		},
		{
			name:  "filtering on exact account address",
			query: query.Match("address", "player:0"),
		},
		{
			name:  "filtering on account address segment",
			query: query.Match("address", ":0"),
		},
		{
			name:  "filtering on metadata",
			query: query.Match("metadata[level]", "2"),
		},
	}

	for _, t := range testCases {
		t := t
		b.Run("aggregating balance "+t.name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				ret, err := store.GetAggregatedBalances(ctx, NewGetAggregatedBalancesQuery(PaginatedQueryOptions[PITFilter]{
					PageSize:     10,
					QueryBuilder: t.query,
				}))
				require.NoError(b, err)
				if !t.allowEmptyResponse && len(ret) == 0 {
					require.Fail(b, "response should not be empty")
				}
			}
		})
	}
}
