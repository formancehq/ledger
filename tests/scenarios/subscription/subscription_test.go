//go:build scenario

package subscription

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/scenario"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/scenarios/scenariotest"
)

// TestSubscriptionBillingCycle models a SaaS billing system with 50 subscribers
// over 3 monthly cycles, with under-funded failures, credits, adjustments,
// revenue recognition, and period closes.
// Generates ~200 Apply calls to trigger ~4 cache rotations (threshold=50).
func TestSubscriptionBillingCycle(t *testing.T) {
	const (
		ledger         = scenario.SubscriptionLedger
		numSubscribers = 50
		numCycles      = 3
		numUnderFunded = 5
	)

	sc := scenariotest.SetupSingleNode(t, scenariotest.HTTPPort+2, scenariotest.GRPCPort+2)
	ctx, client := sc.Ctx(), sc.Client

	// Subscriber tiers
	type subscriber struct {
		id       int
		tier     string
		amount   int64
		fundedAt int64 // wallet funding amount per cycle; 0 means under-funded
	}

	// Build subscribers: first numUnderFunded are under-funded
	subscribers := make([]subscriber, 0, numSubscribers)
	tiers := []struct {
		name   string
		amount int64
		fund   int64
	}{
		{"basic", 1000, 5000},
		{"pro", 2500, 10000},
		{"enterprise", 5000, 20000},
	}
	for i := 1; i <= numSubscribers; i++ {
		tier := tiers[(i-1)%len(tiers)]
		fund := tier.fund
		if i <= numUnderFunded {
			fund = tier.amount / (numCycles + 1) // under-funded: even after all cycles, total < charge
		}
		subscribers = append(subscribers, subscriber{
			id:       i,
			tier:     tier.name,
			amount:   tier.amount,
			fundedAt: fund,
		})
	}

	var (
		totalDeferred    = new(big.Int)
		totalRecognized  = new(big.Int)
		adjustmentAmount = big.NewInt(500)
		firstFundTxID    uint64 // captured in first FundWallets for typed tx metadata test
	)

	// --- Phase 1: Setup & Numscript Library ---
	t.Run("Setup", func(t *testing.T) {
		scenariotest.ApplyActions(t, ctx, client, scenario.SubscriptionSetupActions()...)
	})

	// --- Billing Cycles ---
	for cycle := 1; cycle <= numCycles; cycle++ {
		cycleName := fmt.Sprintf("Cycle%d", cycle)

		// Fund wallets at the start of each cycle
		t.Run(fmt.Sprintf("%s/FundWallets", cycleName), func(t *testing.T) {
			reqs := make([]*servicepb.Request, 0, numSubscribers)
			for _, sub := range subscribers {
				reqs = append(reqs, actions.CreateScriptRefTransactionAction(ledger, "fund_wallet", "1.0.0", map[string]string{
					"subscriber": fmt.Sprintf("subscriber:%d", sub.id),
					"amount":     fmt.Sprintf("USD/2 %d", sub.fundedAt),
				}, nil))
			}
			resp := scenariotest.ApplyActions(t, ctx, client, reqs...)

			// Capture first fund tx ID on cycle 1 for later typed metadata test
			if cycle == 1 {
				firstFundTxID = scenariotest.GetCreatedTransactionID(t, resp)
			}
		})

		// Monthly billing
		t.Run(fmt.Sprintf("%s/Billing", cycleName), func(t *testing.T) {
			var successCount, failCount int
			for _, sub := range subscribers {
				err := scenariotest.ApplyActionsExpectError(ctx, client,
					actions.CreateScriptRefTransactionAction(ledger, "charge_subscription", "1.0.0", map[string]string{
						"subscriber": fmt.Sprintf("subscriber:%d", sub.id),
						"amount":     fmt.Sprintf("USD/2 %d", sub.amount),
					}, nil),
				)

				if sub.fundedAt < sub.amount {
					require.Error(t, err, "expected insufficient funds for subscriber:%d cycle:%d",
						sub.id, cycle)
					failCount++
				} else {
					require.NoError(t, err, "unexpected error for subscriber:%d cycle:%d", sub.id, cycle)
					totalDeferred.Add(totalDeferred, big.NewInt(sub.amount))
					successCount++
				}
			}

			expectedSuccess := numSubscribers - numUnderFunded
			require.Equal(t, expectedSuccess, successCount, "cycle %d: wrong success count", cycle)
			require.Equal(t, numUnderFunded, failCount, "cycle %d: wrong fail count", cycle)
		})

		// Credits to 2 subscribers
		t.Run(fmt.Sprintf("%s/Credits", cycleName), func(t *testing.T) {
			scenariotest.ApplyActions(t, ctx, client,
				actions.CreateScriptRefTransactionAction(ledger, "issue_credit", "1.0.0", map[string]string{
					"subscriber": fmt.Sprintf("subscriber:%d", 6+cycle),
					"amount":     "USD/2 200",
				}, nil),
				actions.CreateScriptRefTransactionAction(ledger, "issue_credit", "1.0.0", map[string]string{
					"subscriber": fmt.Sprintf("subscriber:%d", 10+cycle),
					"amount":     "USD/2 300",
				}, nil),
			)
		})

		// Typed metadata: save typed account metadata on first cycle
		if cycle == 1 {
			t.Run(fmt.Sprintf("%s/TypedMetadata", cycleName), func(t *testing.T) {
				scenariotest.ApplyActions(t, ctx, client,
					actions.SaveTypedAccountMetadataAction(ledger, "subscriber:6", map[string]*commonpb.MetadataValue{
						"subscriber_plan": {Type: &commonpb.MetadataValue_StringValue{StringValue: "pro"}},
						"billing_cycle":   {Type: &commonpb.MetadataValue_IntValue{IntValue: 1}},
						"retention_score": {Type: &commonpb.MetadataValue_IntValue{IntValue: 85}},
					}),
					actions.SaveTypedAccountMetadataAction(ledger, "subscriber:7", map[string]*commonpb.MetadataValue{
						"subscriber_plan": {Type: &commonpb.MetadataValue_StringValue{StringValue: "enterprise"}},
						"retention_score": {Type: &commonpb.MetadataValue_IntValue{IntValue: 92}},
					}),
				)

				// Verify typed metadata was stored
				acct, err := actions.GetAccount(ctx, client, ledger, "subscriber:6")
				require.NoError(t, err)
				plan := actions.FindMetadataValue(acct.Metadata, "subscriber_plan")
				require.NotNil(t, plan, "subscriber_plan should exist")
				require.Equal(t, "pro", plan.GetStringValue())
				score := actions.FindMetadataValue(acct.Metadata, "retention_score")
				require.NotNil(t, score, "retention_score should exist")
				require.Equal(t, int64(85), score.GetIntValue())
			})
		}

		// Typed transaction metadata: set typed metadata on a transaction (first cycle only)
		if cycle == 1 {
			t.Run(fmt.Sprintf("%s/TypedTxMetadata", cycleName), func(t *testing.T) {
				require.NotZero(t, firstFundTxID, "should have captured first fund tx ID")
				scenariotest.ApplyActions(t, ctx, client,
					actions.SaveTypedTransactionMetadataAction(ledger, firstFundTxID, map[string]*commonpb.MetadataValue{
						"billing_cycle":   {Type: &commonpb.MetadataValue_IntValue{IntValue: 1}},
						"subscriber_plan": {Type: &commonpb.MetadataValue_StringValue{StringValue: "initial_fund"}},
					}),
				)

				// Verify typed metadata on the transaction
				txResp, err := actions.GetTransaction(ctx, client, ledger, firstFundTxID)
				require.NoError(t, err, "GetTransaction failed")
				tx := txResp.GetTransaction()
				require.NotNil(t, tx, "transaction should exist")
				cycleVal := actions.FindMetadataValue(tx.Metadata, "billing_cycle")
				require.NotNil(t, cycleVal, "billing_cycle metadata should exist on tx")
				require.Equal(t, int64(1), cycleVal.GetIntValue())
			})
		}

		// Revenue adjustment
		t.Run(fmt.Sprintf("%s/Adjustment", cycleName), func(t *testing.T) {
			scenariotest.ApplyActions(t, ctx, client,
				actions.CreateScriptRefTransactionAction(ledger, "adjust_revenue", "1.0.0", map[string]string{
					"amount": fmt.Sprintf("USD/2 %d", adjustmentAmount.Int64()),
				}, nil),
			)
			totalDeferred.Sub(totalDeferred, adjustmentAmount)
		})

		// Revenue recognition
		t.Run(fmt.Sprintf("%s/RevenueRecognition", cycleName), func(t *testing.T) {
			recognizeAmt := new(big.Int).Set(totalDeferred)
			scenariotest.ApplyActions(t, ctx, client,
				actions.CreateScriptRefTransactionAction(ledger, "recognize_revenue", "1.0.0", map[string]string{
					"amount": fmt.Sprintf("USD/2 %d", recognizeAmt.Int64()),
				}, nil),
			)
			totalRecognized.Add(totalRecognized, recognizeAmt)
			totalDeferred.Sub(totalDeferred, recognizeAmt)

			scenariotest.CheckAccountBalance(t, ctx, client, ledger, "revenue:deferred", "USD/2", totalDeferred)
		})

		// Period close
		t.Run(fmt.Sprintf("%s/PeriodClose", cycleName), func(t *testing.T) {
			scenariotest.ClosePeriodAndWait(t, ctx, client, "period close timed out cycle %d", cycle)
			scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		})
	}

	// --- Metadata Schema Verification ---
	t.Run("MetadataSchemaStatus", func(t *testing.T) {
		resp, err := actions.GetMetadataSchemaStatus(ctx, client, ledger)
		require.NoError(t, err, "GetMetadataSchemaStatus failed")

		// Verify declared account fields persist through billing cycles
		acctFields := resp.GetAccountFields()
		require.Contains(t, acctFields, "subscriber_plan", "subscriber_plan should be declared")
		require.Contains(t, acctFields, "billing_cycle", "billing_cycle should be declared")
		require.Contains(t, acctFields, "retention_score", "retention_score should be declared")
		t.Logf("MetadataSchema: %d account fields declared", len(acctFields))
	})

	// --- Prepared Queries ---
	t.Run("PreparedQueries", func(t *testing.T) {
		// Wait for index backfill to complete
		require.Eventually(t, func() bool {
			indexStatus, err := actions.GetIndexStatus(ctx, client)
			if err != nil {
				return false
			}
			return indexStatus.GetLag() == 0 && len(indexStatus.GetBackfillProgress()) == 0
		}, 15*time.Second, 200*time.Millisecond, "index backfill should complete")

		// 1. Parameterized address prefix — reusable across different prefixes
		resp, err := actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": actions.StringParam("subscriber:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(subscriber:) failed")
		require.Equal(t, numSubscribers, len(resp.GetCursor().GetAccountData()),
			"prefix=subscriber: should return all subscribers")

		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": actions.StringParam("revenue:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(revenue:) failed")
		require.GreaterOrEqual(t, len(resp.GetCursor().GetAccountData()), 2,
			"prefix=revenue: should return deferred + recognized + adjustment")

		// 2. Parameterized string metadata — filter by subscriber plan
		// Query for "pro" subscribers — subscriber:6 was tagged with plan=pro
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "by-plan",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"plan_value": actions.StringParam("pro")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(plan=pro) failed")
		require.GreaterOrEqual(t, len(resp.GetCursor().GetAccountData()), 1,
			"should find at least 1 account with plan=pro")

		// Query for "enterprise" subscribers — subscriber:7 was tagged
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "by-plan",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"plan_value": actions.StringParam("enterprise")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(plan=enterprise) failed")
		require.GreaterOrEqual(t, len(resp.GetCursor().GetAccountData()), 1,
			"should find at least 1 account with plan=enterprise")

		// Query for a plan nobody has — should return 0
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "by-plan",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"plan_value": actions.StringParam("nonexistent")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(plan=nonexistent) failed")
		require.Empty(t, resp.GetCursor().GetAccountData(),
			"nonexistent plan should return 0 accounts")

		// 3. Parameterized int64 range metadata — filter by retention_score range
		// Query score >= 90 (subscriber:7 has 92) — retry until index is ready
		require.Eventually(t, func() bool {
			resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "high-retention",
				commonpb.QueryMode_QUERY_MODE_LIST, 100,
				map[string]*commonpb.ParameterValue{
					"min_score": actions.Int64Param(90),
					"max_score": actions.Int64Param(100),
				},
			)
			return err == nil
		}, 15*time.Second, 200*time.Millisecond, "ExecutePreparedQueryWithParams(score 90-100) should succeed once index is ready")
		require.GreaterOrEqual(t, len(resp.GetCursor().GetAccountData()), 1,
			"should find at least 1 high-retention subscriber (score >= 90)")

		// Query score 80-90 (subscriber:6 has 85)
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "high-retention",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{
				"min_score": actions.Int64Param(80),
				"max_score": actions.Int64Param(90),
			},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(score 80-90) failed")
		require.GreaterOrEqual(t, len(resp.GetCursor().GetAccountData()), 1,
			"should find at least 1 subscriber with score 80-90")

		// Query score 0-50 — nobody has scores that low
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "high-retention",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{
				"min_score": actions.Int64Param(0),
				"max_score": actions.Int64Param(50),
			},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(score 0-50) failed")
		require.Empty(t, resp.GetCursor().GetAccountData(),
			"no subscribers should have score in 0-50 range")

		// Cleanup
		require.NoError(t, actions.DeletePreparedQuery(ctx, client, ledger, "accounts-by-prefix"))
		require.NoError(t, actions.DeletePreparedQuery(ctx, client, ledger, "by-plan"))
		require.NoError(t, actions.DeletePreparedQuery(ctx, client, ledger, "high-retention"))
	})

	// --- BUG: CreateAccountMetadataIndex fails under cache pressure ---
	// After ~200 Apply calls (4+ cache rotations with threshold=50), the
	// CreateAccountMetadataIndex after high cache pressure (200+ Apply calls, 4+ rotations).
	// Regression test: LedgerInfo must be preloaded for Apply orders to survive cache eviction.
	t.Run("CreateAccountMetadataIndexUnderLoad", func(t *testing.T) {
		// Drop the index created in SetupQueriesAndIndexes, then re-create under load
		scenariotest.ApplyActions(t, ctx, client,
			actions.DropAccountMetadataIndexAction(ledger, "subscriber_plan"),
		)
		scenariotest.ApplyActions(t, ctx, client,
			actions.CreateAccountMetadataIndexAction(ledger, "subscriber_plan"),
		)

		// Cleanup
		scenariotest.ApplyActions(t, ctx, client,
			actions.DropAccountMetadataIndexAction(ledger, "subscriber_plan"),
		)
	})

	// --- Final Invariants ---
	t.Run("FinalInvariants", func(t *testing.T) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "revenue:recognized", "USD/2", totalRecognized)

		// Under-funded subscribers should have their accumulated funding (never charged)
		for _, sub := range subscribers {
			if sub.fundedAt < sub.amount {
				expected := big.NewInt(sub.fundedAt * int64(numCycles))
				scenariotest.CheckAccountBalance(t, ctx, client, ledger,
					fmt.Sprintf("subscriber:%d", sub.id), "USD/2", expected)
			}
		}
	})

	// --- Audit Trail ---
	// Per cycle: 50 funds + 45 charges + 2 credits + 1 adjust + 1 recognize = 99
	expectedTxCount := numCycles * (numSubscribers + (numSubscribers - numUnderFunded) + 2 + 1 + 1)
	t.Run("AuditTrail", func(t *testing.T) {
		scenariotest.CheckAuditTrail(t, ctx, client, []scenariotest.AuditExpectation{{
			Ledger:           ledger,
			MinTransactions:  expectedTxCount,
			ExpectedReverted: 0,
		}})
	})

	// --- Tail phases: StoreCheck, Backup, Restart+Verify, BackupRestore+Verify ---
	scenariotest.RunPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "revenue:recognized", "USD/2", totalRecognized)

		for _, sub := range subscribers {
			if sub.fundedAt < sub.amount {
				expected := big.NewInt(sub.fundedAt * int64(numCycles))
				scenariotest.CheckAccountBalance(t, ctx, client, ledger,
					fmt.Sprintf("subscriber:%d", sub.id), "USD/2", expected)
			}
		}

		scenariotest.CheckAuditTrail(t, ctx, client, []scenariotest.AuditExpectation{{
			Ledger:           ledger,
			MinTransactions:  expectedTxCount,
			ExpectedReverted: 0,
		}})
	})
}
