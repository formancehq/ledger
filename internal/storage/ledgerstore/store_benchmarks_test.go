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
var batch = flag.Int("batch", 1000, "logs batching")

type scenarioInfo struct {
	nbAccounts int
}

type scenario struct {
	name  string
	setup func(ctx context.Context, b *testing.B, store *Store) *scenarioInfo
}

var scenarios = []scenario{
	{
		name: "nominal",
		setup: func(ctx context.Context, b *testing.B, store *Store) *scenarioInfo {
			var lastLog *ledger.ChainedLog
			for i := 0; i < *nbTransactions/(*batch); i++ {
				logs := make([]*ledger.ChainedLog, 0)
				appendLog := func(log *ledger.Log) {
					chainedLog := log.ChainLog(lastLog)
					logs = append(logs, chainedLog)
					lastLog = chainedLog
				}
				for j := 0; j < (*batch); j += 2 {
					provision := big.NewInt(10000)
					itemPrice := provision.Div(provision, big.NewInt(2))
					fees := itemPrice.Div(itemPrice, big.NewInt(100)) // 1%

					appendLog(ledger.NewTransactionLog(
						ledger.NewTransaction().WithPostings(ledger.NewPosting(
							"world", fmt.Sprintf("player:%d", j/2), "USD/2", provision,
						)).WithID(big.NewInt(int64(i*(*batch)+j))),
						map[string]metadata.Metadata{},
					))
					appendLog(ledger.NewTransactionLog(
						ledger.NewTransaction().WithPostings(
							ledger.NewPosting(fmt.Sprintf("player:%d", j/2), "seller", "USD/2", itemPrice),
							ledger.NewPosting("seller", "fees", "USD/2", fees),
						).WithID(big.NewInt(int64(i*(*batch)+j+1))),
						map[string]metadata.Metadata{},
					))
					status := "pending"
					if j%8 == 0 {
						status = "terminated"
					}
					appendLog(ledger.NewSetMetadataLog(ledger.Now(), ledger.SetMetadataLogPayload{
						TargetType: ledger.MetaTargetTypeTransaction,
						TargetID:   big.NewInt(int64(i*(*batch) + j + 1)),
						Metadata: map[string]string{
							"status": status,
						},
					}))
				}
				require.NoError(b, store.InsertLogs(ctx, logs...))
			}

			nbAccounts := *batch / 2

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

			return &scenarioInfo{
				nbAccounts: nbAccounts,
			}
		},
	},
}

func BenchmarkList(b *testing.B) {

	ctx := logging.TestingContext()
	hooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		hooks = append(hooks, bunexplain.NewExplainHook())
	}

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			store := newLedgerStore(b, hooks...)
			info := scenario.setup(ctx, b, store)

			_, err := store.db.Exec("VACUUM ANALYZE")
			require.NoError(b, err)

			b.Run("transactions", func(b *testing.B) {
				benchmarksReadTransactions(b, ctx, store, info)
			})
			b.Run("accounts", func(b *testing.B) {
				benchmarksReadAccounts(b, ctx, store)
			})
			b.Run("aggregates", func(b *testing.B) {
				benchmarksGetAggregatedBalances(b, ctx, store)
			})
		})
	}
}

func benchmarksReadTransactions(b *testing.B, ctx context.Context, store *Store, info *scenarioInfo) {
	type testCase struct {
		name                   string
		query                  query.Builder
		allowEmptyResponse     bool
		expandVolumes          bool
		expandEffectiveVolumes bool
	}

	testCases := []testCase{
		{
			name: "no query",
		},
		{
			name:  "using an exact address",
			query: query.Match("account", fmt.Sprintf("player:%d", info.nbAccounts-1)), // Last inserted account
		},
		{
			name:  "using an address segment",
			query: query.Match("account", fmt.Sprintf(":%d", info.nbAccounts-1)),
		},
		{
			name:  "using a metadata metadata",
			query: query.Match("metadata[status]", "terminated"),
		},
		{
			name:               "using non existent account by exact address",
			query:              query.Match("account", fmt.Sprintf("player:%d", info)),
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
		b.Run(t.name, func(b *testing.B) {
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

func benchmarksReadAccounts(b *testing.B, ctx context.Context, store *Store) {
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
		b.Run(t.name, func(b *testing.B) {
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

func benchmarksGetAggregatedBalances(b *testing.B, ctx context.Context, store *Store) {
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
		b.Run(t.name, func(b *testing.B) {
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
