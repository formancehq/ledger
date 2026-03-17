//go:build scenario

package stressinvariants

import (
	"fmt"
	"math/big"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/tests/scenarios/scenariotest"
)

// TestStressInvariants is a pure stress test focusing on cache eviction and
// invariant verification under high transaction volume.
// 100 bulk deposits + 400 trading iterations = ~530 Apply calls.
// With cache-rotation-threshold=50: ~10 rotations, ~53 snapshots (threshold=10).
func TestStressInvariants(t *testing.T) {
	const (
		ledger      = "stress"
		numAccounts = 100
		numTrades   = 400
		depositAmt  = 1_000_000 // USD/2 per account
	)

	sc := scenariotest.SetupSingleNode(t, scenariotest.HTTPPort+3, scenariotest.GRPCPort+3)
	ctx, client := sc.Ctx(), sc.Client

	// Track reverted trade indices to avoid double-revert
	tradeTxIDs := make([]uint64, 0, numTrades)
	revertedTrades := make(map[int]bool)

	// --- Phase 1: Setup ---
	t.Run("Setup", func(t *testing.T) {
		scenariotest.ApplyActions(t, ctx, client,
			testutil.CreateLedgerAction(ledger, nil),
			// Account types: enforce address patterns
			testutil.AddAccountTypeAction(ledger, "trader", "trader:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "exchange-fees", "exchange:fees", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "exchange-withdrawals", "exchange:withdrawals", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.SaveNumscriptWithVersionAction(ledger, "deposit", `vars {
  account $account
  monetary $amount
}
send $amount (
  source = @world
  destination = $account
)`, "1.0.0"),
			testutil.SaveNumscriptWithVersionAction(ledger, "trade", `vars {
  account $buyer
  account $seller
  monetary $amount
}
send $amount (
  source = $buyer
  destination = {
    1/100 to @exchange:fees
    remaining to $seller
  }
)`, "1.0.0"),
			testutil.SaveNumscriptWithVersionAction(ledger, "withdraw", `vars {
  account $account
  monetary $amount
}
send $amount (
  source = $account
  destination = @exchange:withdrawals
)`, "1.0.0"),
		)
	})

	// --- Phase 2: Bulk Deposits (100 Apply calls) ---
	t.Run("BulkDeposits", func(t *testing.T) {
		actions := make([]*servicepb.Request, 0, numAccounts)
		for i := 1; i <= numAccounts; i++ {
			actions = append(actions, testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
				"account": fmt.Sprintf("trader:%d", i),
				"amount":  fmt.Sprintf("USD/2 %d", depositAmt),
			}, nil))
		}
		scenariotest.ApplyActions(t, ctx, client, actions...)

		// Spot check a few accounts
		for _, i := range []int{1, 25, 50, 75, 100} {
			scenariotest.CheckAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("trader:%d", i), "USD/2", big.NewInt(depositAmt))
		}
	})

	// --- Phase 3: Trading Loop (400 Apply calls + periodic reads/reverts/closes) ---
	t.Run("TradingLoop", func(t *testing.T) {
		for i := 0; i < numTrades; i++ {
			buyer := 1 + i%numAccounts
			seller := 1 + (i+numAccounts/2)%numAccounts
			if seller == buyer {
				seller = 1 + (seller)%numAccounts
			}
			amount := int64(100 + (i%50)*10)

			resp := scenariotest.ApplyActions(t, ctx, client,
				testutil.CreateScriptRefTransactionAction(ledger, "trade", "1.0.0", map[string]string{
					"buyer":  fmt.Sprintf("trader:%d", buyer),
					"seller": fmt.Sprintf("trader:%d", seller),
					"amount": fmt.Sprintf("USD/2 %d", amount),
				}, nil),
			)
			tradeTxIDs = append(tradeTxIDs, scenariotest.GetCreatedTransactionID(t, resp))

			// Every 20 trades: read 5 random-ish accounts (cache hit/miss exercise)
			if (i+1)%20 == 0 {
				for j := 0; j < 5; j++ {
					acctIdx := 1 + (i*7+j*13)%numAccounts
					_, err := testutil.GetAccount(ctx, client, ledger, fmt.Sprintf("trader:%d", acctIdx))
					require.NoError(t, err, "failed to read trader:%d at trade %d", acctIdx, i)
				}
			}

			// Every 40 trades: revert a recent trade
			if (i+1)%40 == 0 && len(tradeTxIDs) > 10 {
				revertIdx := len(tradeTxIDs) - 10
				if !revertedTrades[revertIdx] {
					scenariotest.ApplyActions(t, ctx, client,
						testutil.RevertTransactionAction(ledger, tradeTxIDs[revertIdx], true, false, nil),
					)
					revertedTrades[revertIdx] = true
				}
			}

			// Every 80 trades: close period + check double-entry
			if (i+1)%80 == 0 {
				scenariotest.ClosePeriodAndWait(t, ctx, client, "period close timed out at trade %d", i)
				scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
			}
		}
	})

	// --- Phase 3b: Audit Entries after Trading ---
	// Note: audit entries require explicit SetAuditConfig(true). The stress test
	// does not enable audit logging, so we only verify the RPC works (empty is OK).
	t.Run("AuditEntriesAfterTrading", func(t *testing.T) {
		entries, err := testutil.ListAuditEntries(ctx, client, false)
		require.NoError(t, err, "ListAuditEntries RPC should succeed")
		t.Logf("Audit entries after trading: %d total", len(entries))
	})

	// --- Phase 4: Final Invariants ---
	t.Run("FinalInvariants", func(t *testing.T) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})
		scenariotest.CheckPositiveBalance(t, ctx, client, ledger, "exchange:fees", "USD/2")

		// GetLedgerStats: verify counts
		stats, err := testutil.GetLedgerStats(ctx, client, ledger)
		require.NoError(t, err, "GetLedgerStats failed")
		require.Greater(t, stats.GetAccountCount(), uint64(0), "should have accounts")
		require.Greater(t, stats.GetTransactionCount(), uint64(0), "should have transactions")
		t.Logf("LedgerStats: %d accounts, %d transactions",
			stats.GetAccountCount(), stats.GetTransactionCount())
	})

	// --- Phase 4b: Monitoring RPCs ---
	t.Run("MonitoringRPCs", func(t *testing.T) {
		// GetStoreMetrics
		storeMetrics, err := testutil.GetStoreMetrics(ctx, client)
		require.NoError(t, err, "GetStoreMetrics failed")
		require.True(t, storeMetrics.GetAvailable(), "store metrics should be available")
		require.NotNil(t, storeMetrics.GetMetrics(), "store metrics should not be nil")
		t.Logf("StoreMetrics: available=%v", storeMetrics.GetAvailable())

		// GetReadIndexMetrics
		readMetrics, err := testutil.GetReadIndexMetrics(ctx, client)
		require.NoError(t, err, "GetReadIndexMetrics failed")
		require.True(t, readMetrics.GetAvailable(), "read index metrics should be available")
		t.Logf("ReadIndexMetrics: available=%v", readMetrics.GetAvailable())

		// GetIndexStatus
		indexStatus, err := testutil.GetIndexStatus(ctx, client)
		require.NoError(t, err, "GetIndexStatus failed")
		require.Greater(t, indexStatus.GetLastIndexedSequence(), uint64(0),
			"last indexed sequence should be > 0")
		t.Logf("IndexStatus: lastIndexed=%d, lastLog=%d, lag=%d",
			indexStatus.GetLastIndexedSequence(), indexStatus.GetLastLogSequence(), indexStatus.GetLag())
	})

	// --- Phase 4c: Discovery ---
	t.Run("Discovery", func(t *testing.T) {
		resp, err := testutil.Discovery(ctx, client)
		require.NoError(t, err, "Discovery failed")
		require.NotNil(t, resp, "Discovery response should not be nil")
		// ResponseSigning may be nil if not configured — that's OK
		t.Logf("Discovery: responseSigning=%v", resp.GetResponseSigning() != nil)
	})

	// --- Phase 5: Audit Trail ---
	numRevertedTrades := len(revertedTrades)
	t.Run("AuditTrail", func(t *testing.T) {
		// 100 deposits + 400 trades + revert txs
		scenariotest.CheckAuditTrail(t, ctx, client, []scenariotest.AuditExpectation{{
			Ledger:           ledger,
			MinTransactions:  numAccounts + numTrades + numRevertedTrades,
			ExpectedReverted: numRevertedTrades,
		}})
	})

	// --- Tail phases: StoreCheck, Backup, Restart+Verify, BackupRestore+Verify ---
	scenariotest.RunPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})
		scenariotest.CheckPositiveBalance(t, ctx, client, ledger, "exchange:fees", "USD/2")

		scenariotest.CheckAuditTrail(t, ctx, client, []scenariotest.AuditExpectation{{
			Ledger:           ledger,
			MinTransactions:  numAccounts + numTrades + numRevertedTrades,
			ExpectedReverted: numRevertedTrades,
		}})
	})
}
