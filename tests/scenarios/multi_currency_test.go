//go:build scenario

package scenarios

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"github.com/stretchr/testify/require"
)

// TestMultiCurrencyTreasury models a corporate treasury with multiple currencies
// and FX operations through a clearing account.
// 20 FX ops across 5 currency pairs, 50 vendor payments, 2 intercalated period closes.
// Generates ~100 Apply calls to trigger ~2 cache rotations (threshold=50).
func TestMultiCurrencyTreasury(t *testing.T) {
	const ledger = "treasury"

	sc := setupSingleNode(t, scenarioHTTPPort+1, scenarioGRPCPort+1)
	ctx, client := sc.ctx, sc.Client

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
		applyActions(t, ctx, client,
			testutil.CreateLedgerAction(ledger, nil),
			// Account types: enforce address patterns
			testutil.AddAccountTypeAction(ledger, "treasury", "treasury:{currency}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "fx-clearing", "fx:clearing", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "vendor", "vendor:{name}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.SaveNumscriptWithVersionAction("fund_account", `vars {
  account $account
  monetary $amount
}
send $amount (
  source = @world
  destination = $account
)`, "1.0.0"),
			testutil.SaveNumscriptWithVersionAction("fx_convert", `vars {
  account $source_account
  account $clearing_account
  monetary $amount
}
send $amount (
  source = $source_account
  destination = $clearing_account
)`, "1.0.0"),
			testutil.SaveNumscriptWithVersionAction("vendor_payment", `vars {
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
		applyActions(t, ctx, client,
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
			applyActions(t, ctx, client,
				testutil.CreateScriptRefTransactionAction(ledger, "fx_convert", "1.0.0", map[string]string{
					"source_account":   fx.sourceAccount,
					"clearing_account": "fx:clearing",
					"amount":           fmt.Sprintf("%s %d", fx.sourceAsset, fx.sourceAmount),
				}, nil),
			)

			// Leg 2: fx:clearing → target (in target currency, force because clearing doesn't have target currency)
			applyActions(t, ctx, client,
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
				closePeriodAndWait(t, ctx, client, "period close timed out at FX op %d", i)
				checkDoubleEntryBalance(t, ctx, client, ledger)
			}
		}
	})

	// --- Phase 2b: Force Script + Indexes ---
	t.Run("ForceScriptAndIndexes", func(t *testing.T) {
		// Force script transaction: bypass balance check with Numscript
		applyActions(t, ctx, client,
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
		applyActions(t, ctx, client,
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
		applyActions(t, ctx, client,
			testutil.DropBuiltinTxIndexAction(ledger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
		)
	})

	// --- Phase 3: Vendor Payments (50 Apply calls) ---
	t.Run("VendorPayments", func(t *testing.T) {
		applyVendorPayments := func(payments []vendorPayment) {
			for _, vp := range payments {
				applyActions(t, ctx, client,
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

	// --- Phase 4: Close & Reconciliation ---
	t.Run("CloseAndReconciliation", func(t *testing.T) {
		closePeriodAndWait(t, ctx, client, "period close timed out")
		checkDoubleEntryBalance(t, ctx, client, ledger)

		// Verify all tracked balances
		for acct, assets := range balances {
			for asset, expected := range assets {
				checkAccountBalance(t, ctx, client, ledger, acct, asset, expected)
			}
		}

		checkNoNegativeBalances(t, ctx, client, ledger, []string{"world", "fx:clearing"})
	})

	// --- Audit Trail ---
	t.Run("AuditTrail", func(t *testing.T) {
		// 3 funds + 20 FX×2 legs + 1 force script + 30 EUR payments + 20 GBP payments = 94
		checkAuditTrail(t, ctx, client, []auditExpectation{{
			ledger:           ledger,
			minTransactions:  3 + len(fxOps)*2 + 1 + len(eurPayments) + len(gbpPayments),
			expectedReverted: 0,
		}})
	})

	// --- Tail phases: StoreCheck, Backup, Restart+Verify, BackupRestore+Verify ---
	runPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		checkDoubleEntryBalance(t, ctx, client, ledger)
		checkNoNegativeBalances(t, ctx, client, ledger, []string{"world", "fx:clearing"})

		for acct, assets := range balances {
			for asset, expected := range assets {
				checkAccountBalance(t, ctx, client, ledger, acct, asset, expected)
			}
		}

		checkAuditTrail(t, ctx, client, []auditExpectation{{
			ledger:           ledger,
			minTransactions:  3 + len(fxOps)*2 + len(eurPayments) + len(gbpPayments),
			expectedReverted: 0,
		}})
	})
}
