//go:build it

package ledgerstore

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"os"
	"testing"
	"text/tabwriter"

	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/bun/bunexplain"
	"github.com/formancehq/go-libs/pointer"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/metadata"
	"github.com/formancehq/go-libs/query"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
)

var nbTransactions = flag.Int("transactions", 10000, "number of transactions to create")
var batch = flag.Int("batch", 1000, "logs batching")
var ledgers = flag.Int("ledgers", 100, "number of ledger for multi ledgers benchmarks")

type bunContextHook struct{}

func (b bunContextHook) BeforeQuery(ctx context.Context, event *bun.QueryEvent) context.Context {
	hooks := ctx.Value("hooks")
	if hooks == nil {
		return ctx
	}

	for _, hook := range hooks.([]bun.QueryHook) {
		ctx = hook.BeforeQuery(ctx, event)
	}

	return ctx
}

func (b bunContextHook) AfterQuery(ctx context.Context, event *bun.QueryEvent) {
	hooks := ctx.Value("hooks")
	if hooks == nil {
		return
	}

	for _, hook := range hooks.([]bun.QueryHook) {
		hook.AfterQuery(ctx, event)
	}

	return
}

var _ bun.QueryHook = &bunContextHook{}

func contextWithHook(ctx context.Context, hooks ...bun.QueryHook) context.Context {
	return context.WithValue(ctx, "hooks", hooks)
}

type scenarioInfo struct {
	nbAccounts int
}

type scenario struct {
	name  string
	setup func(ctx context.Context, b *testing.B, store *Store) *scenarioInfo
}

var now = time.Now()

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
						ledger.NewTransaction().
							WithPostings(ledger.NewPosting(
								"world", fmt.Sprintf("player:%d", j/2), "USD/2", provision,
							)).
							WithID(big.NewInt(int64(i*(*batch)+j))).
							WithDate(now.Add(time.Minute*time.Duration(i*(*batch)+j))),
						map[string]metadata.Metadata{},
					))
					appendLog(ledger.NewTransactionLog(
						ledger.NewTransaction().
							WithPostings(
								ledger.NewPosting(fmt.Sprintf("player:%d", j/2), "seller", "USD/2", itemPrice),
								ledger.NewPosting("seller", "fees", "USD/2", fees),
							).
							WithID(big.NewInt(int64(i*(*batch)+j+1))).
							WithDate(now.Add(time.Minute*time.Duration(i*(*batch)+j))),
						map[string]metadata.Metadata{},
					))
					status := "pending"
					if j%8 == 0 {
						status = "terminated"
					}
					appendLog(ledger.NewSetMetadataLog(now.Add(time.Minute*time.Duration(i*(*batch)+j)), ledger.SetMetadataLogPayload{
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
				lastLog = ledger.NewSetMetadataLog(now, ledger.SetMetadataLogPayload{
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
	{
		name: "multi-ledger",
		setup: func(ctx context.Context, b *testing.B, store *Store) *scenarioInfo {
			var lastLog *ledger.ChainedLog

			nbAccounts := *batch / 2
			loadData := func(store *Store) {
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
							ledger.NewTransaction().
								WithPostings(ledger.NewPosting(
									"world", fmt.Sprintf("player:%d", j/2), "USD/2", provision,
								)).
								WithID(big.NewInt(int64(i*(*batch)+j))).
								WithDate(now.Add(time.Minute*time.Duration(i*(*batch)+j))),
							map[string]metadata.Metadata{},
						))
						appendLog(ledger.NewTransactionLog(
							ledger.NewTransaction().
								WithPostings(
									ledger.NewPosting(fmt.Sprintf("player:%d", j/2), "seller", "USD/2", itemPrice),
									ledger.NewPosting("seller", "fees", "USD/2", fees),
								).
								WithID(big.NewInt(int64(i*(*batch)+j+1))).
								WithDate(now.Add(time.Minute*time.Duration(i*(*batch)+j))),
							map[string]metadata.Metadata{},
						))
						status := "pending"
						if j%8 == 0 {
							status = "terminated"
						}
						appendLog(ledger.NewSetMetadataLog(now.Add(time.Minute*time.Duration(i*(*batch)+j)), ledger.SetMetadataLogPayload{
							TargetType: ledger.MetaTargetTypeTransaction,
							TargetID:   big.NewInt(int64(i*(*batch) + j + 1)),
							Metadata: map[string]string{
								"status": status,
							},
						}))
					}
					require.NoError(b, store.InsertLogs(ctx, logs...))
				}

				for i := 0; i < nbAccounts; i++ {
					lastLog = ledger.NewSetMetadataLog(now, ledger.SetMetadataLogPayload{
						TargetType: ledger.MetaTargetTypeAccount,
						TargetID:   fmt.Sprintf("player:%d", i),
						Metadata: map[string]string{
							"level": fmt.Sprint(i % 4),
						},
					}).ChainLog(lastLog)
					require.NoError(b, store.InsertLogs(ctx, lastLog))
				}
			}

			for i := 0; i < *ledgers; i++ {
				store := newLedgerStore(b)
				loadData(store)
			}
			loadData(store)

			return &scenarioInfo{
				nbAccounts: nbAccounts,
			}
		},
	},
}

func reportMetrics(ctx context.Context, b *testing.B, store *Store) {
	type stat struct {
		RelID        string `bun:"relid"`
		IndexRelID   string `bun:"indexrelid"`
		RelName      string `bun:"relname"`
		IndexRelName string `bun:"indexrelname"`
		IdxScan      int    `bun:"idxscan"`
		IdxTupRead   int    `bun:"idx_tup_read"`
		IdxTupFetch  int    `bun:"idx_tup_fetch"`
	}
	ret := make([]stat, 0)
	err := store.GetDB().NewSelect().
		Table("pg_stat_user_indexes").
		Where("schemaname = ?", store.name).
		Scan(ctx, &ret)
	require.NoError(b, err)

	tabWriter := tabwriter.NewWriter(os.Stderr, 8, 8, 0, '\t', 0)
	defer func() {
		require.NoError(b, tabWriter.Flush())
	}()
	_, err = fmt.Fprintf(tabWriter, "IndexRelName\tIdxScan\tIdxTypRead\tIdxTupFetch\r\n")
	require.NoError(b, err)

	_, err = fmt.Fprintf(tabWriter, "---\t---\r\n")
	require.NoError(b, err)

	for _, s := range ret {
		_, err := fmt.Fprintf(tabWriter, "%s\t%d\t%d\t%d\r\n", s.IndexRelName, s.IdxScan, s.IdxTupRead, s.IdxTupFetch)
		require.NoError(b, err)
	}
}

func reportTableSizes(ctx context.Context, b *testing.B, store *Store) {

	tabWriter := tabwriter.NewWriter(os.Stderr, 12, 8, 0, '\t', 0)
	defer func() {
		require.NoError(b, tabWriter.Flush())
	}()
	_, err := fmt.Fprintf(tabWriter, "Table\tTotal size\tTable size\tRelation size\tIndexes size\tMain size\tFSM size\tVM size\tInit size\r\n")
	require.NoError(b, err)

	_, err = fmt.Fprintf(tabWriter, "---\t---\t---\t---\t---\t---\t---\t---\r\n")
	require.NoError(b, err)

	for _, table := range []string{
		"transactions", "accounts", "moves", "logs", "transactions_metadata", "accounts_metadata",
	} {
		totalRelationSize := ""
		err := store.GetDB().DB.QueryRowContext(ctx, fmt.Sprintf(`select pg_size_pretty(pg_total_relation_size('%s'))`, table)).
			Scan(&totalRelationSize)
		require.NoError(b, err)

		tableSize := ""
		err = store.GetDB().DB.QueryRowContext(ctx, fmt.Sprintf(`select pg_size_pretty(pg_table_size('%s'))`, table)).
			Scan(&tableSize)
		require.NoError(b, err)

		relationSize := ""
		err = store.GetDB().DB.QueryRowContext(ctx, fmt.Sprintf(`select pg_size_pretty(pg_relation_size('%s'))`, table)).
			Scan(&relationSize)
		require.NoError(b, err)

		indexesSize := ""
		err = store.GetDB().DB.QueryRowContext(ctx, fmt.Sprintf(`select pg_size_pretty(pg_indexes_size('%s'))`, table)).
			Scan(&indexesSize)
		require.NoError(b, err)

		mainSize := ""
		err = store.GetDB().DB.QueryRowContext(ctx, fmt.Sprintf(`select pg_size_pretty(pg_relation_size('%s', 'main'))`, table)).
			Scan(&mainSize)
		require.NoError(b, err)

		fsmSize := ""
		err = store.GetDB().DB.QueryRowContext(ctx, fmt.Sprintf(`select pg_size_pretty(pg_relation_size('%s', 'fsm'))`, table)).
			Scan(&fsmSize)
		require.NoError(b, err)

		vmSize := ""
		err = store.GetDB().DB.QueryRowContext(ctx, fmt.Sprintf(`select pg_size_pretty(pg_relation_size('%s', 'vm'))`, table)).
			Scan(&vmSize)
		require.NoError(b, err)

		initSize := ""
		err = store.GetDB().DB.QueryRowContext(ctx, fmt.Sprintf(`select pg_size_pretty(pg_relation_size('%s', 'init'))`, table)).
			Scan(&initSize)
		require.NoError(b, err)

		_, err = fmt.Fprintf(tabWriter, "%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\r\n",
			table, totalRelationSize, tableSize, relationSize, indexesSize, mainSize, fsmSize, vmSize, initSize)
		require.NoError(b, err)
	}
}

func BenchmarkList(b *testing.B) {

	ctx := logging.TestingContext()

	for _, scenario := range scenarios {
		b.Run(scenario.name, func(b *testing.B) {
			store := newLedgerStore(b, &bunContextHook{})
			info := scenario.setup(ctx, b, store)

			defer func() {
				if testing.Verbose() {
					reportMetrics(ctx, b, store)
					reportTableSizes(ctx, b, store)
				}
			}()

			_, err := store.GetDB().Exec("VACUUM FULL ANALYZE")
			require.NoError(b, err)

			runAllWithPIT := func(b *testing.B, pit *time.Time) {
				b.Run("transactions", func(b *testing.B) {
					benchmarksReadTransactions(b, ctx, store, info, pit)
				})
				b.Run("accounts", func(b *testing.B) {
					benchmarksReadAccounts(b, ctx, store, pit)
				})
				b.Run("aggregates", func(b *testing.B) {
					benchmarksGetAggregatedBalances(b, ctx, store, pit)
				})
			}
			runAllWithPIT(b, nil)
			b.Run("using pit", func(b *testing.B) {
				// Use pit with the more recent, this way we force the storage to use a join
				// Doing this allowing to test the worst case
				runAllWithPIT(b, pointer.For(now.Add(time.Minute*time.Duration(*nbTransactions))))
			})
		})
	}
}

func benchmarksReadTransactions(b *testing.B, ctx context.Context, store *Store, info *scenarioInfo, pit *time.Time) {
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
			var q GetTransactionsQuery
			for i := 0; i < b.N; i++ {
				q = NewGetTransactionsQuery(PaginatedQueryOptions[PITFilterWithVolumes]{
					PageSize:     100,
					QueryBuilder: t.query,
					Options: PITFilterWithVolumes{
						PITFilter: PITFilter{
							PIT: pit,
						},
					},
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

			explainRequest(ctx, b, func(ctx context.Context) {
				_, err := store.GetTransactions(ctx, q)
				require.NoError(b, err)
			})
		})
	}
}

func benchmarksReadAccounts(b *testing.B, ctx context.Context, store *Store, pit *time.Time) {
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
			var q GetAccountsQuery
			for i := 0; i < b.N; i++ {
				q = NewGetAccountsQuery(PaginatedQueryOptions[PITFilterWithVolumes]{
					PageSize:     100,
					QueryBuilder: t.query,
					Options: PITFilterWithVolumes{
						PITFilter: PITFilter{
							PIT: pit,
						},
					},
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

			explainRequest(ctx, b, func(ctx context.Context) {
				_, err := store.GetAccountsWithVolumes(ctx, q)
				require.NoError(b, err)
			})
		})
	}
}

func benchmarksGetAggregatedBalances(b *testing.B, ctx context.Context, store *Store, pit *time.Time) {
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
			var q GetAggregatedBalanceQuery
			for i := 0; i < b.N; i++ {
				q = NewGetAggregatedBalancesQuery(PITFilter{
					PIT: pit,
				}, t.query, false)
				ret, err := store.GetAggregatedBalances(ctx, q)
				require.NoError(b, err)
				if !t.allowEmptyResponse && len(ret) == 0 {
					require.Fail(b, "response should not be empty")
				}
			}

			explainRequest(ctx, b, func(ctx context.Context) {
				_, err := store.GetAggregatedBalances(ctx, q)
				require.NoError(b, err)
			})
		})
	}
}

func explainRequest(ctx context.Context, b *testing.B, f func(ctx context.Context)) {
	var (
		explained     string
		jsonExplained string
	)
	additionalHooks := make([]bun.QueryHook, 0)
	if testing.Verbose() {
		additionalHooks = append(additionalHooks, bunexplain.NewExplainHook(bunexplain.WithListener(func(data string) {
			explained = data
		})))
	}
	additionalHooks = append(additionalHooks, bunexplain.NewExplainHook(
		bunexplain.WithListener(func(data string) {
			jsonExplained = data
		}),
		bunexplain.WithJSONFormat(),
	))
	ctx = contextWithHook(ctx, additionalHooks...)
	f(ctx)

	if testing.Verbose() {
		fmt.Println(explained)
	}
	jsonQueryPlan := make([]any, 0)

	require.NoError(b, json.Unmarshal([]byte(jsonExplained), &jsonQueryPlan))
	b.ReportMetric(jsonQueryPlan[0].(map[string]any)["Plan"].(map[string]any)["Total Cost"].(float64), "cost")
}
