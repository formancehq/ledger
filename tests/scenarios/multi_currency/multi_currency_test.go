//go:build scenario

package multicurrency

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/tests/scenarios/scenariotest"
)

// TestMultiCurrencyTreasury models a corporate treasury with multiple currencies
// and FX operations through a clearing account.
// 20 FX ops across 5 currency pairs, 50 vendor payments, 2 intercalated period closes.
// Generates ~100 Apply calls to trigger ~2 cache rotations (threshold=50).
func TestMultiCurrencyTreasury(t *testing.T) {
	const ledger = "treasury"

	sc := scenariotest.SetupSingleNode(t, scenariotest.HTTPPort+1, scenariotest.GRPCPort+1)
	ctx, client := sc.Ctx(), sc.Client

	// FX operation definition
	type fxOp struct {
		sourceAccount string
		sourceAsset   string
		sourceAmount  int64
		targetAccount string
		targetAsset   string
		targetAmount  int64
	}

	// 5 currency pairs, 4 ops each = 20 FX ops (each = 2 Apply calls = 40 total)
	fxOps := []fxOp{
		// USD → EUR
		{"treasury:usd", "USD/2", 10_000, "treasury:eur", "EUR/2", 9_200},
		{"treasury:usd", "USD/2", 15_000, "treasury:eur", "EUR/2", 13_800},
		{"treasury:usd", "USD/2", 8_000, "treasury:eur", "EUR/2", 7_360},
		{"treasury:usd", "USD/2", 20_000, "treasury:eur", "EUR/2", 18_500},
		// EUR → GBP
		{"treasury:eur", "EUR/2", 5_000, "treasury:gbp", "GBP/2", 4_300},
		{"treasury:eur", "EUR/2", 8_000, "treasury:gbp", "GBP/2", 6_880},
		{"treasury:eur", "EUR/2", 3_000, "treasury:gbp", "GBP/2", 2_580},
		{"treasury:eur", "EUR/2", 12_000, "treasury:gbp", "GBP/2", 10_320},
		// GBP → USD
		{"treasury:gbp", "GBP/2", 6_000, "treasury:usd", "USD/2", 7_620},
		{"treasury:gbp", "GBP/2", 4_000, "treasury:usd", "USD/2", 5_080},
		{"treasury:gbp", "GBP/2", 2_000, "treasury:usd", "USD/2", 2_540},
		{"treasury:gbp", "GBP/2", 3_000, "treasury:usd", "USD/2", 3_810},
		// USD → GBP
		{"treasury:usd", "USD/2", 7_000, "treasury:gbp", "GBP/2", 5_530},
		{"treasury:usd", "USD/2", 9_000, "treasury:gbp", "GBP/2", 7_110},
		{"treasury:usd", "USD/2", 5_000, "treasury:gbp", "GBP/2", 3_950},
		{"treasury:usd", "USD/2", 11_000, "treasury:gbp", "GBP/2", 8_690},
		// EUR → USD
		{"treasury:eur", "EUR/2", 6_000, "treasury:usd", "USD/2", 6_540},
		{"treasury:eur", "EUR/2", 10_000, "treasury:usd", "USD/2", 10_900},
		{"treasury:eur", "EUR/2", 4_000, "treasury:usd", "USD/2", 4_360},
		{"treasury:eur", "EUR/2", 7_000, "treasury:usd", "USD/2", 7_630},
	}

	// Vendor payments: 30 EUR + 20 GBP = 50 total
	type vendorPayment struct {
		treasury string
		vendor   string
		asset    string
		amount   int64
	}

	var eurPayments []vendorPayment
	eurVendors := []string{"vendor:acme", "vendor:globex", "vendor:initech", "vendor:umbrella", "vendor:stark"}
	for i := 0; i < 30; i++ {
		eurPayments = append(eurPayments, vendorPayment{
			treasury: "treasury:eur",
			vendor:   eurVendors[i%len(eurVendors)],
			asset:    "EUR/2",
			amount:   int64(500 + i*100),
		})
	}

	var gbpPayments []vendorPayment
	gbpVendors := []string{"vendor:brit-co", "vendor:london-ltd", "vendor:windsor", "vendor:thames"}
	for i := 0; i < 20; i++ {
		gbpPayments = append(gbpPayments, vendorPayment{
			treasury: "treasury:gbp",
			vendor:   gbpVendors[i%len(gbpVendors)],
			asset:    "GBP/2",
			amount:   int64(400 + i*80),
		})
	}

	// Track expected balances per account per asset
	balances := map[string]map[string]*big.Int{
		"treasury:usd": {"USD/2": big.NewInt(1_000_000)},
		"treasury:eur": {"EUR/2": big.NewInt(500_000)},
		"treasury:gbp": {"GBP/2": big.NewInt(300_000)},
	}
	getBalance := func(acct, asset string) *big.Int {
		if balances[acct] == nil {
			balances[acct] = make(map[string]*big.Int)
		}
		if balances[acct][asset] == nil {
			balances[acct][asset] = new(big.Int)
		}
		return balances[acct][asset]
	}
	adjustBalance := func(acct, asset string, delta int64) {
		bal := getBalance(acct, asset)
		bal.Add(bal, big.NewInt(delta))
	}

	// --- Phase 1: Setup & Numscript Library ---
	t.Run("Setup", func(t *testing.T) {
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateLedgerAction(ledger, nil),
			// Account types: enforce address patterns
			testutil.AddAccountTypeAction(ledger, "treasury", "treasury:{currency}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "fx-clearing", "fx:clearing", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "vendor", "vendor:{name}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.SaveNumscriptWithVersionAction(ledger, "fund_account", `vars {
  account $account
  monetary $amount
}
send $amount (
  source = @world
  destination = $account
)`, "1.0.0"),
			testutil.SaveNumscriptWithVersionAction(ledger, "fx_convert", `vars {
  account $source_account
  account $clearing_account
  monetary $amount
}
send $amount (
  source = $source_account
  destination = $clearing_account
)`, "1.0.0"),
			testutil.SaveNumscriptWithVersionAction(ledger, "vendor_payment", `vars {
  account $treasury
  account $vendor
  monetary $amount
}
send $amount (
  source = $treasury
  destination = $vendor
)`, "1.0.0"),
		)

		// Fund treasury accounts
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateScriptRefTransactionAction(ledger, "fund_account", "1.0.0", map[string]string{
				"account": "treasury:usd",
				"amount":  "USD/2 1000000",
			}, nil),
			testutil.CreateScriptRefTransactionAction(ledger, "fund_account", "1.0.0", map[string]string{
				"account": "treasury:eur",
				"amount":  "EUR/2 500000",
			}, nil),
			testutil.CreateScriptRefTransactionAction(ledger, "fund_account", "1.0.0", map[string]string{
				"account": "treasury:gbp",
				"amount":  "GBP/2 300000",
			}, nil),
		)
	})

	// --- Phase 2: FX Operations (20 ops = 40 Apply calls) ---
	t.Run("FXOperations", func(t *testing.T) {
		for i, fx := range fxOps {
			// Leg 1: source → fx:clearing (in source currency, balance-checked)
			scenariotest.ApplyActions(t, ctx, client,
				testutil.CreateScriptRefTransactionAction(ledger, "fx_convert", "1.0.0", map[string]string{
					"source_account":   fx.sourceAccount,
					"clearing_account": "fx:clearing",
					"amount":           fmt.Sprintf("%s %d", fx.sourceAsset, fx.sourceAmount),
				}, nil),
			)

			// Leg 2: fx:clearing → target (in target currency, force because clearing doesn't have target currency)
			scenariotest.ApplyActions(t, ctx, client,
				testutil.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					testutil.NewPosting("fx:clearing", fx.targetAccount, big.NewInt(fx.targetAmount), fx.targetAsset),
				}, nil),
			)

			adjustBalance(fx.sourceAccount, fx.sourceAsset, -fx.sourceAmount)
			adjustBalance("fx:clearing", fx.sourceAsset, fx.sourceAmount)
			adjustBalance("fx:clearing", fx.targetAsset, -fx.targetAmount)
			adjustBalance(fx.targetAccount, fx.targetAsset, fx.targetAmount)

			// Close period after every 10 FX ops
			if (i+1)%10 == 0 {
				scenariotest.ClosePeriodAndWait(t, ctx, client, "period close timed out at FX op %d", i)
				scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
			}
		}
	})

	// --- Phase 2b: Force Script + Indexes ---
	t.Run("ForceScriptAndIndexes", func(t *testing.T) {
		// Force script transaction: bypass balance check with Numscript
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateForceScriptTransactionAction(ledger,
				`vars {
  account $src
  account $dst
  monetary $amount
}
send $amount (
  source = $src
  destination = $dst
)`,
				map[string]string{
					"src":    "fx:clearing",
					"dst":    "treasury:usd",
					"amount": "USD/2 1",
				},
				map[string]string{"force_script": "true"},
			),
		)
		adjustBalance("fx:clearing", "USD/2", -1)
		adjustBalance("treasury:usd", "USD/2", 1)

		// Create a builtin transaction index (timestamp)
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateBuiltinTxIndexAction(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
		)

		// Wait for the index to become READY
		require.Eventually(t, func() bool {
			info, err := testutil.GetLedger(ctx, client, ledger)
			if err != nil {
				return false
			}
			bi := info.GetBuiltinIndexes()
			return bi != nil && bi.GetTimestampStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
		}, 30*time.Second, 200*time.Millisecond, "timestamp index should become READY")

		// Drop the index
		scenariotest.ApplyActions(t, ctx, client,
			testutil.DropBuiltinTxIndexAction(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
		)

		// Create a builtin transaction index (inserted_at / creation date)
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateBuiltinTxIndexAction(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
		)

		// Wait for the index to become READY
		require.Eventually(t, func() bool {
			info, err := testutil.GetLedger(ctx, client, ledger)
			if err != nil {
				return false
			}
			bi := info.GetBuiltinIndexes()
			return bi != nil && bi.GetInsertedAtStatus() == commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY
		}, 30*time.Second, 200*time.Millisecond, "inserted_at index should become READY")

		// Drop the index
		scenariotest.ApplyActions(t, ctx, client,
			testutil.DropBuiltinTxIndexAction(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
		)
	})

	// --- Phase 3: Vendor Payments (50 Apply calls) ---
	t.Run("VendorPayments", func(t *testing.T) {
		applyVendorPayments := func(payments []vendorPayment) {
			for _, vp := range payments {
				scenariotest.ApplyActions(t, ctx, client,
					testutil.CreateScriptRefTransactionAction(ledger, "vendor_payment", "1.0.0", map[string]string{
						"treasury": vp.treasury,
						"vendor":   vp.vendor,
						"amount":   fmt.Sprintf("%s %d", vp.asset, vp.amount),
					}, nil),
				)
				adjustBalance(vp.treasury, vp.asset, -vp.amount)
				adjustBalance(vp.vendor, vp.asset, vp.amount)
			}
		}

		applyVendorPayments(eurPayments)
		applyVendorPayments(gbpPayments)
	})

	// --- Phase 3b: AnalyzeAccounts + AnalyzeTransactions ---
	t.Run("Analytics", func(t *testing.T) {
		// AnalyzeAccounts
		acctResult, err := testutil.AnalyzeAccounts(ctx, client, ledger, 3)
		require.NoError(t, err, "AnalyzeAccounts failed")
		require.NotNil(t, acctResult, "AnalyzeAccounts result should not be nil")
		require.Greater(t, acctResult.GetTotalAccounts(), uint64(0), "should have accounts")
		require.NotEmpty(t, acctResult.GetPatterns(), "should have patterns")
		t.Logf("AnalyzeAccounts: %d total accounts, %d patterns",
			acctResult.GetTotalAccounts(), len(acctResult.GetPatterns()))

		// AnalyzeTransactions
		txResult, err := testutil.AnalyzeTransactions(ctx, client, ledger)
		require.NoError(t, err, "AnalyzeTransactions failed")
		require.NotNil(t, txResult, "AnalyzeTransactions result should not be nil")
		require.Greater(t, txResult.GetTotalTransactions(), uint64(0), "should have transactions")
		require.NotEmpty(t, txResult.GetFlowPatterns(), "should have flow patterns")
		t.Logf("AnalyzeTransactions: %d total transactions, %d flow patterns",
			txResult.GetTotalTransactions(), len(txResult.GetFlowPatterns()))
	})

	// --- Phase 3c: Prepared Queries with typed parameters ---
	t.Run("PreparedQueries", func(t *testing.T) {
		// 1. Parameterized address prefix — reusable query for different account types
		err := testutil.CreatePreparedQuery(ctx, client, "accounts-by-prefix", ledger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			testutil.ParamAddressPrefixFilter("prefix"),
		)
		require.NoError(t, err, "CreatePreparedQuery(accounts-by-prefix) failed")

		// Query for all treasury accounts
		resp, err := testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": testutil.StringParam("treasury:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(treasury:) failed")
		require.Equal(t, 3, len(resp.GetCursor().GetAccountData()),
			"should find 3 treasury accounts (usd, eur, gbp)")

		// Query for all vendor accounts
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": testutil.StringParam("vendor:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(vendor:) failed")
		// 5 EUR vendors + 4 GBP vendors = 9 unique vendors
		require.Equal(t, len(eurVendors)+len(gbpVendors), len(resp.GetCursor().GetAccountData()),
			"should find all unique vendor accounts")

		// Query for fx accounts
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": testutil.StringParam("fx:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(fx:) failed")
		require.Equal(t, 1, len(resp.GetCursor().GetAccountData()),
			"should find exactly 1 fx account (fx:clearing)")

		// 2. Parameterized exact address — find specific accounts
		err = testutil.CreatePreparedQuery(ctx, client, "account-exact", ledger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			testutil.ParamAddressExactFilter("addr"),
		)
		require.NoError(t, err, "CreatePreparedQuery(account-exact) failed")

		// Query for a specific vendor
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "account-exact",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"addr": testutil.StringParam("vendor:acme")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(vendor:acme) failed")
		require.Equal(t, 1, len(resp.GetCursor().GetAccountData()),
			"exact match should return exactly 1 account")

		// Query for nonexistent account
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "account-exact",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"addr": testutil.StringParam("vendor:nonexistent")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(nonexistent) failed")
		require.Empty(t, resp.GetCursor().GetAccountData(),
			"nonexistent vendor should return 0 results")

		// 3. Aggregate volumes with parameterized prefix — verify per-account-type volumes
		err = testutil.CreatePreparedQuery(ctx, client, "volumes-by-prefix", ledger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			testutil.ParamAddressPrefixFilter("prefix"),
		)
		require.NoError(t, err, "CreatePreparedQuery(volumes-by-prefix) failed")

		// Aggregate volumes for treasury accounts
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "volumes-by-prefix",
			commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES, 0,
			map[string]*commonpb.ParameterValue{"prefix": testutil.StringParam("treasury:")},
		)
		require.NoError(t, err, "AGGREGATE_VOLUMES(treasury:) failed")
		aggResult := resp.GetAggregate()
		require.NotNil(t, aggResult, "expected aggregate result")
		require.NotEmpty(t, aggResult.GetVolumes(), "treasury accounts should have volumes")

		// Aggregate volumes for vendor accounts
		resp, err = testutil.ExecutePreparedQueryWithParams(ctx, client, ledger, "volumes-by-prefix",
			commonpb.QueryMode_QUERY_MODE_AGGREGATE_VOLUMES, 0,
			map[string]*commonpb.ParameterValue{"prefix": testutil.StringParam("vendor:")},
		)
		require.NoError(t, err, "AGGREGATE_VOLUMES(vendor:) failed")
		aggResult = resp.GetAggregate()
		require.NotNil(t, aggResult, "expected aggregate result for vendors")
		require.NotEmpty(t, aggResult.GetVolumes(), "vendor accounts should have volumes")

		// Cleanup
		require.NoError(t, testutil.DeletePreparedQuery(ctx, client, ledger, "accounts-by-prefix"))
		require.NoError(t, testutil.DeletePreparedQuery(ctx, client, ledger, "account-exact"))
		require.NoError(t, testutil.DeletePreparedQuery(ctx, client, ledger, "volumes-by-prefix"))
	})

	// --- Phase 4: Close & Reconciliation ---
	t.Run("CloseAndReconciliation", func(t *testing.T) {
		scenariotest.ClosePeriodAndWait(t, ctx, client, "period close timed out")
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)

		// Verify all tracked balances
		for acct, assets := range balances {
			for asset, expected := range assets {
				scenariotest.CheckAccountBalance(t, ctx, client, ledger, acct, asset, expected)
			}
		}

		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world", "fx:clearing"})
	})

	// --- Audit Trail ---
	t.Run("AuditTrail", func(t *testing.T) {
		// 3 funds + 20 FX×2 legs + 1 force script + 30 EUR payments + 20 GBP payments = 94
		scenariotest.CheckAuditTrail(t, ctx, client, []scenariotest.AuditExpectation{{
			Ledger:           ledger,
			MinTransactions:  3 + len(fxOps)*2 + 1 + len(eurPayments) + len(gbpPayments),
			ExpectedReverted: 0,
		}})
	})

	// --- Tail phases: StoreCheck, Backup, Restart+Verify, BackupRestore+Verify ---
	scenariotest.RunPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world", "fx:clearing"})

		for acct, assets := range balances {
			for asset, expected := range assets {
				scenariotest.CheckAccountBalance(t, ctx, client, ledger, acct, asset, expected)
			}
		}

		scenariotest.CheckAuditTrail(t, ctx, client, []scenariotest.AuditExpectation{{
			Ledger:           ledger,
			MinTransactions:  3 + len(fxOps)*2 + len(eurPayments) + len(gbpPayments),
			ExpectedReverted: 0,
		}})
	})
}
