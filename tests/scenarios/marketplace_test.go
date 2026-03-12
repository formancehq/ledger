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

// TestMarketplaceLifecycle models a high-volume e-commerce marketplace:
// 50 customers, 10 merchants, 200 purchases with fees, periodic closes,
// reverts, cold-account reads, and merchant payouts.
// Generates ~270 Apply calls to trigger ~5 cache rotations (threshold=50).
func TestMarketplaceLifecycle(t *testing.T) {
	const (
		ledger       = "marketplace"
		numCustomers = 50
		numMerchants = 10
		numPurchases = 200
		numReverts   = 10
		depositAmt   = 1_000_000 // USD/2 cents — large enough for all purchases
		feePercent   = 3
	)

	sc := setupSingleNode(t, scenarioHTTPPort, scenarioGRPCPort)
	ctx, client := sc.ctx, sc.Client

	// Track expected balances
	customerBalance := make(map[int]*big.Int, numCustomers)
	for i := 1; i <= numCustomers; i++ {
		customerBalance[i] = big.NewInt(depositAmt)
	}
	merchantBalance := make(map[int]*big.Int, numMerchants)
	for i := 1; i <= numMerchants; i++ {
		merchantBalance[i] = new(big.Int)
	}
	totalFees := new(big.Int)

	// Track purchase transaction IDs for reverts
	purchaseTxIDs := make([]uint64, 0, numPurchases)
	// Track purchase details for balance adjustment on revert
	type purchaseRecord struct {
		customer int
		merchant int
		amount   int64
		reverted bool
	}
	purchaseRecords := make([]purchaseRecord, 0, numPurchases)

	// --- Phase 1: Setup & Numscript Library ---
	t.Run("Setup", func(t *testing.T) {
		applyActions(t, ctx, client,
			testutil.CreateLedgerAction(ledger, nil),
			// Account types: enforce address patterns
			testutil.AddAccountTypeAction(ledger, "customer", "customer:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "merchant", "merchant:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "platform-fees", "platform:fees", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.AddAccountTypeAction(ledger, "platform-payouts", "platform:payouts", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
			testutil.SaveNumscriptWithVersionAction("deposit", `vars {
  account $customer
  monetary $amount
}
send $amount (
  source = @world
  destination = $customer
)`, "1.0.0"),
			testutil.SaveNumscriptWithVersionAction("purchase", `vars {
  account $customer
  account $merchant
  monetary $amount
}
send $amount (
  source = $customer
  destination = {
    3/100 to @platform:fees
    remaining to $merchant
  }
)`, "1.0.0"),
			testutil.SaveNumscriptWithVersionAction("payout", `vars {
  account $merchant
  monetary $amount
}
send $amount (
  source = $merchant
  destination = @platform:payouts
)`, "1.0.0"),
		)
	})

	// --- Phase 1a: Verify Setup via Reads ---
	t.Run("VerifySetup", func(t *testing.T) {
		// GetLedger: verify config and account types
		ledgerInfo, err := testutil.GetLedger(ctx, client, ledger)
		require.NoError(t, err)
		require.Equal(t, ledger, ledgerInfo.GetName())
		require.Len(t, ledgerInfo.GetAccountTypes(), 4, "should have 4 account types after setup")

		// ListNumscripts + GetNumscript: verify 3 scripts registered
		scripts, err := testutil.ListNumscripts(ctx, client)
		require.NoError(t, err)
		require.Len(t, scripts, 3, "should have 3 numscripts (deposit, purchase, payout)")

		for _, name := range []string{"deposit", "purchase", "payout"} {
			info, err := testutil.GetNumscript(ctx, client, name, "1.0.0")
			require.NoError(t, err, "GetNumscript(%s) failed", name)
			require.Equal(t, name, info.GetName())
			require.Equal(t, "1.0.0", info.GetVersion())
			require.NotEmpty(t, info.GetContent())
		}
	})

	// --- Phase 1b: Account Type Lifecycle (Add/Update/Remove) ---
	t.Run("AccountTypeLifecycle", func(t *testing.T) {
		// Verify account types are present after setup
		ledgers, err := testutil.ListLedgers(ctx, client)
		require.NoError(t, err)
		ledgerInfo := ledgers[ledger]
		require.NotNil(t, ledgerInfo, "ledger %q should exist", ledger)
		require.Len(t, ledgerInfo.GetAccountTypes(), 4, "should have 4 account types")

		// Add a temporary type
		applyActions(t, ctx, client,
			testutil.AddAccountTypeAction(ledger, "temp-type", "temp:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		)

		// Verify it was added
		ledgers, err = testutil.ListLedgers(ctx, client)
		require.NoError(t, err)
		require.Len(t, ledgers[ledger].GetAccountTypes(), 5, "should have 5 account types after add")

		// Update enforcement mode to AUDIT
		applyActions(t, ctx, client,
			testutil.UpdateAccountTypeAction(ledger, "temp-type", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
		)

		// Verify the update
		ledgers, err = testutil.ListLedgers(ctx, client)
		require.NoError(t, err)
		tempType := ledgers[ledger].GetAccountTypes()["temp-type"]
		require.NotNil(t, tempType)
		require.Equal(t, commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT, tempType.GetEnforcementMode())

		// Remove the type
		applyActions(t, ctx, client,
			testutil.RemoveAccountTypeAction(ledger, "temp-type"),
		)

		// Verify removal
		ledgers, err = testutil.ListLedgers(ctx, client)
		require.NoError(t, err)
		require.Len(t, ledgers[ledger].GetAccountTypes(), 4, "should have 4 account types after remove")

		// Account type violation: using an address that doesn't match any registered type
		// should fail when enforcement is STRICT
		violationErr := applyActionsExpectError(ctx, client,
			testutil.CreateTransactionAction(ledger, []*commonpb.Posting{
				testutil.NewPosting("world", "unknown:address", big.NewInt(100), "USD/2"),
			}, nil, nil),
		)
		require.Error(t, violationErr, "expected account type violation for unknown:address")
	})

	// --- Phase 2: Customer Deposits (50 Apply calls) ---
	t.Run("CustomerDeposits", func(t *testing.T) {
		actions := make([]*servicepb.Request, 0, numCustomers)
		for i := 1; i <= numCustomers; i++ {
			actions = append(actions, testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
				"customer": fmt.Sprintf("customer:%d", i),
				"amount":   fmt.Sprintf("USD/2 %d", depositAmt),
			}, nil))
		}
		applyActions(t, ctx, client, actions...)
	})

	// --- Phase 3: Purchases with Fees (200 iterations, with periodic closes and reads) ---
	t.Run("PurchasesWithFees", func(t *testing.T) {
		for i := 0; i < numPurchases; i++ {
			customer := 1 + i%numCustomers
			merchant := 1 + i%numMerchants
			amount := int64(1000 + i*100)

			resp := applyActions(t, ctx, client,
				testutil.CreateScriptRefTransactionAction(ledger, "purchase", "1.0.0", map[string]string{
					"customer": fmt.Sprintf("customer:%d", customer),
					"merchant": fmt.Sprintf("merchant:%d", merchant),
					"amount":   fmt.Sprintf("USD/2 %d", amount),
				}, nil),
			)
			txID := getCreatedTransactionID(t, resp)
			purchaseTxIDs = append(purchaseTxIDs, txID)
			purchaseRecords = append(purchaseRecords, purchaseRecord{
				customer: customer,
				merchant: merchant,
				amount:   amount,
			})

			fee := amount * feePercent / 100
			net := amount - fee
			customerBalance[customer].Sub(customerBalance[customer], big.NewInt(amount))
			merchantBalance[merchant].Add(merchantBalance[merchant], big.NewInt(net))
			totalFees.Add(totalFees, big.NewInt(fee))

			// Every 60 transactions: close period + check double-entry
			if (i+1)%60 == 0 {
				closePeriodAndWait(t, ctx, client, "period close timed out at purchase %d", i)
				checkDoubleEntryBalance(t, ctx, client, ledger)
			}

			// Every 20 transactions: read a "cold" account (not recently touched)
			if (i+1)%20 == 0 {
				coldCustomer := 1 + (i+numCustomers/2)%numCustomers
				_, err := testutil.GetAccount(ctx, client, ledger, fmt.Sprintf("customer:%d", coldCustomer))
				require.NoError(t, err, "failed to read cold account customer:%d", coldCustomer)
			}
		}

		// Spot-check platform fees
		checkAccountBalance(t, ctx, client, ledger, "platform:fees", "USD/2", totalFees)

		// GetTransaction: verify a purchase transaction has correct structure
		txResp, err := testutil.GetTransaction(ctx, client, ledger, purchaseTxIDs[0])
		require.NoError(t, err, "GetTransaction failed for first purchase")
		tx := txResp.GetTransaction()
		require.NotNil(t, tx)
		require.Equal(t, purchaseTxIDs[0], tx.GetId())
		require.NotEmpty(t, tx.GetPostings(), "purchase transaction should have postings")
	})

	// --- Phase 3b: WithTimestamp + WithExpandVolumes ---
	t.Run("TimestampAndExpandVolumes", func(t *testing.T) {
		// Antidated transactions: create 2 transactions with past timestamps
		// Use platform:payouts as destination to avoid interfering with tracked customer balances.
		pastTime1 := time.Now().Add(-24 * time.Hour)
		pastTime2 := time.Now().Add(-48 * time.Hour)

		applyActions(t, ctx, client,
			testutil.WithTimestamp(
				testutil.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					testutil.NewPosting("world", "platform:payouts", big.NewInt(100), "USD/2"),
				}, map[string]string{"backdated": "true"}),
				pastTime1,
			),
		)

		applyActions(t, ctx, client,
			testutil.WithTimestamp(
				testutil.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					testutil.NewPosting("world", "platform:payouts", big.NewInt(200), "USD/2"),
				}, map[string]string{"backdated": "true"}),
				pastTime2,
			),
		)

		// WithExpandVolumes: verify the response contains volumes
		expandResp := applyActions(t, ctx, client,
			testutil.WithExpandVolumes(
				testutil.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					testutil.NewPosting("world", "platform:payouts", big.NewInt(50), "USD/2"),
				}, nil),
			),
		)

		// The response should contain the log with expanded volumes
		require.NotEmpty(t, expandResp.Logs, "expected at least one log entry")
		applyLog := expandResp.Logs[0].Payload.GetApply()
		require.NotNil(t, applyLog, "expected apply log payload")
		tx := applyLog.Log.Data.GetCreatedTransaction()
		require.NotNil(t, tx, "expected created transaction in log")
		require.NotEmpty(t, tx.PostCommitVolumes, "WithExpandVolumes should populate post-commit volumes")
	})

	// --- Phase 4: Reverts (10 random-ish purchases) ---
	t.Run("Reverts", func(t *testing.T) {
		step := numPurchases / numReverts
		for r := 0; r < numReverts; r++ {
			idx := r * step
			if purchaseRecords[idx].reverted {
				continue
			}
			p := purchaseRecords[idx]

			applyActions(t, ctx, client,
				testutil.RevertTransactionAction(ledger, purchaseTxIDs[idx], true, false, nil),
			)
			purchaseRecords[idx].reverted = true

			fee := p.amount * feePercent / 100
			net := p.amount - fee
			customerBalance[p.customer].Add(customerBalance[p.customer], big.NewInt(p.amount))
			merchantBalance[p.merchant].Sub(merchantBalance[p.merchant], big.NewInt(net))
			totalFees.Sub(totalFees, big.NewInt(fee))
		}

		checkAccountBalance(t, ctx, client, ledger, "platform:fees", "USD/2", totalFees)
	})

	// --- Phase 5: Final Period Close ---
	t.Run("FinalPeriodClose", func(t *testing.T) {
		closePeriodAndWait(t, ctx, client, "final period close timed out")
	})

	// --- Phase 6: Merchant Payouts ---
	t.Run("MerchantPayouts", func(t *testing.T) {
		for i := 1; i <= numMerchants; i++ {
			bal := merchantBalance[i]
			if bal.Sign() <= 0 {
				continue
			}
			applyActions(t, ctx, client,
				testutil.CreateScriptRefTransactionAction(ledger, "payout", "1.0.0", map[string]string{
					"merchant": fmt.Sprintf("merchant:%d", i),
					"amount":   fmt.Sprintf("USD/2 %d", bal.Int64()),
				}, nil),
			)
		}

		for i := 1; i <= numMerchants; i++ {
			checkAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("merchant:%d", i), "USD/2", big.NewInt(0))
		}
	})

	// --- Phase 7: Metadata Operations ---
	t.Run("MetadataOperations", func(t *testing.T) {
		// Add account metadata
		applyActions(t, ctx, client,
			testutil.SaveAccountMetadataAction(ledger, "customer:1", map[string]string{
				"tier": "gold",
				"kyc":  "verified",
			}),
			testutil.SaveAccountMetadataAction(ledger, "merchant:1", map[string]string{
				"category": "electronics",
			}),
		)

		// Verify account metadata was stored
		acct, err := testutil.GetAccount(ctx, client, ledger, "customer:1")
		require.NoError(t, err)
		tier := testutil.FindMetadataValue(acct.Metadata, "tier")
		require.NotNil(t, tier, "tier metadata should exist")

		// Add transaction metadata
		applyActions(t, ctx, client,
			testutil.SaveTransactionMetadataAction(ledger, purchaseTxIDs[0], map[string]string{
				"flagged": "true",
				"reason":  "review",
			}),
		)

		// Delete account metadata
		applyActions(t, ctx, client,
			testutil.DeleteAccountMetadataAction(ledger, "customer:1", "kyc"),
		)
		acct, err = testutil.GetAccount(ctx, client, ledger, "customer:1")
		require.NoError(t, err)
		require.Nil(t, testutil.FindMetadataValue(acct.Metadata, "kyc"), "kyc should be deleted")
		require.NotNil(t, testutil.FindMetadataValue(acct.Metadata, "tier"), "tier should remain")

		// Delete transaction metadata
		applyActions(t, ctx, client,
			testutil.DeleteTransactionMetadataAction(ledger, purchaseTxIDs[0], "reason"),
		)
	})

	// --- Phase 8: Inline Numscript & Raw Postings ---
	t.Run("InlineNumscriptAndRawPostings", func(t *testing.T) {
		// Inline Numscript (not ScriptReference)
		applyActions(t, ctx, client,
			testutil.CreateScriptTransactionAction(ledger, `vars {
  account $src
  account $dst
  monetary $amount
}
send $amount (
  source = $src
  destination = $dst
)`, map[string]string{
				"src":    "customer:1",
				"dst":    "customer:2",
				"amount": "USD/2 100",
			}, nil),
		)
		customerBalance[1].Sub(customerBalance[1], big.NewInt(100))
		customerBalance[2].Add(customerBalance[2], big.NewInt(100))

		// Raw postings (balance-checked, non-force)
		applyActions(t, ctx, client,
			testutil.CreateTransactionAction(ledger, []*commonpb.Posting{
				testutil.NewPosting("customer:2", "customer:3", big.NewInt(50), "USD/2"),
			}, nil, nil),
		)
		customerBalance[2].Sub(customerBalance[2], big.NewInt(50))
		customerBalance[3].Add(customerBalance[3], big.NewInt(50))

		// Raw postings insufficient funds — should fail
		err := applyActionsExpectError(ctx, client,
			testutil.CreateTransactionAction(ledger, []*commonpb.Posting{
				testutil.NewPosting("customer:50", "customer:49", big.NewInt(999_999_999), "USD/2"),
			}, nil, nil),
		)
		require.Error(t, err, "expected insufficient funds error for raw posting")
	})

	// --- Phase 9: Revert (force=false, balance-checked) ---
	t.Run("RevertBalanceChecked", func(t *testing.T) {
		// Create a small transaction, then revert it with force=false.
		// This should succeed because customer:3 has enough balance.
		resp := applyActions(t, ctx, client,
			testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
				"customer": "customer:3",
				"amount":   "USD/2 500",
			}, nil),
		)
		depositTxID := getCreatedTransactionID(t, resp)
		customerBalance[3].Add(customerBalance[3], big.NewInt(500))

		// Revert with force=false — world will receive back, no balance issue
		applyActions(t, ctx, client,
			testutil.RevertTransactionAction(ledger, depositTxID, false, false, nil),
		)
		customerBalance[3].Sub(customerBalance[3], big.NewInt(500))

		// Try to revert the same transaction again — should fail (already reverted)
		err := applyActionsExpectError(ctx, client,
			testutil.RevertTransactionAction(ledger, depositTxID, false, false, nil),
		)
		require.Error(t, err, "expected already-reverted error")
	})

	// --- Phase 10: Idempotency & References ---
	t.Run("IdempotencyAndReferences", func(t *testing.T) {
		// Transaction with a reference
		refAction := testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:1",
			"amount":   "USD/2 100",
		}, nil)
		refAction.GetApply().GetCreateTransaction().Reference = "unique-ref-001"
		applyActions(t, ctx, client, refAction)
		customerBalance[1].Add(customerBalance[1], big.NewInt(100))

		// Duplicate reference — should fail
		refAction2 := testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:2",
			"amount":   "USD/2 100",
		}, nil)
		refAction2.GetApply().GetCreateTransaction().Reference = "unique-ref-001"
		err := applyActionsExpectError(ctx, client, refAction2)
		require.Error(t, err, "expected reference conflict error")

		// Idempotency: create a transaction with an idempotency key
		ikAction := testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:1",
			"amount":   "USD/2 200",
		}, nil)
		ikAction.IdempotencyKey = "ik-deposit-001"
		applyActions(t, ctx, client, ikAction)
		customerBalance[1].Add(customerBalance[1], big.NewInt(200))

		// Idempotency replay: same key + same content → should succeed (return original result)
		ikReplay := testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:1",
			"amount":   "USD/2 200",
		}, nil)
		ikReplay.IdempotencyKey = "ik-deposit-001"
		applyActions(t, ctx, client, ikReplay)
		// No balance change — idempotent replay returns the original log

		// Idempotency conflict: same key + different content
		ikConflict := testutil.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:2",
			"amount":   "USD/2 999",
		}, nil)
		ikConflict.IdempotencyKey = "ik-deposit-001"
		err = applyActionsExpectError(ctx, client, ikConflict)
		require.Error(t, err, "expected idempotency key conflict")
	})

	// --- BUG: Idempotency replay balance drift after restart ---
	// The idempotent replay Raft entry's preloaded data doesn't contain the
	// original key. After restart, the FSM replays the entry as a NEW
	// transaction, creating a duplicate +200 deposit on customer:1.
	// This test captures the expected balance BEFORE restart so we can verify
	// the invariant breaks after restart.
	expectedCustomer1BeforeRestart := new(big.Int).Set(customerBalance[1])

	// --- Phase 11: DeleteNumscript ---
	t.Run("DeleteNumscript", func(t *testing.T) {
		// Save a temporary script, then delete it
		applyActions(t, ctx, client,
			testutil.SaveNumscriptWithVersionAction("temp_script", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @customer:1
)`, "1.0.0"),
		)
		applyActions(t, ctx, client,
			testutil.DeleteNumscriptAction("temp_script"),
		)
	})

	// --- Phase 11b: Prepared Queries ---
	t.Run("PreparedQueries", func(t *testing.T) {
		// Create a prepared query filtering accounts by address prefix
		// (QUERY_TARGET_ACCOUNTS + AddressPrefixFilter works without explicit index creation)
		err := testutil.CreatePreparedQuery(ctx, client, "customer-query", ledger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			testutil.AddressPrefixFilter("customer:"),
		)
		require.NoError(t, err, "CreatePreparedQuery failed")

		// List prepared queries: verify it exists
		queries, err := testutil.ListPreparedQueries(ctx, client, ledger)
		require.NoError(t, err, "ListPreparedQueries failed")
		require.GreaterOrEqual(t, len(queries), 1, "should have at least 1 prepared query")
		var found bool
		for _, q := range queries {
			if q.GetName() == "customer-query" {
				found = true
			}
		}
		require.True(t, found, "customer-query should be in the list")

		// Execute the prepared query
		execResp, err := testutil.ExecutePreparedQuery(ctx, client, ledger, "customer-query",
			commonpb.QueryMode_QUERY_MODE_LIST, 10)
		require.NoError(t, err, "ExecutePreparedQuery failed")
		require.NotNil(t, execResp, "execute response should not be nil")

		// Update the prepared query with a different filter
		err = testutil.UpdatePreparedQuery(ctx, client, ledger, "customer-query",
			testutil.AddressPrefixFilter("merchant:"),
		)
		require.NoError(t, err, "UpdatePreparedQuery failed")

		// Delete the prepared query
		err = testutil.DeletePreparedQuery(ctx, client, ledger, "customer-query")
		require.NoError(t, err, "DeletePreparedQuery failed")

		// Verify it's gone
		queries, err = testutil.ListPreparedQueries(ctx, client, ledger)
		require.NoError(t, err, "ListPreparedQueries after delete failed")
		for _, q := range queries {
			require.NotEqual(t, "customer-query", q.GetName(), "customer-query should be deleted")
		}
	})

	// --- Phase 12: Final Invariants ---
	t.Run("FinalInvariants", func(t *testing.T) {
		checkDoubleEntryBalance(t, ctx, client, ledger)
		checkNoNegativeBalances(t, ctx, client, ledger, []string{"world"})
		checkAccountBalance(t, ctx, client, ledger, "platform:fees", "USD/2", totalFees)

		for i := 1; i <= numCustomers; i++ {
			checkAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("customer:%d", i), "USD/2", customerBalance[i])
		}

		// AggregateVolumes: verify USD/2 is present
		aggResult, err := testutil.AggregateVolumes(ctx, client, ledger)
		require.NoError(t, err, "AggregateVolumes failed")
		require.NotEmpty(t, aggResult.GetVolumes(), "should have aggregated volumes")
		foundUSD := false
		for _, vol := range aggResult.GetVolumes() {
			if vol.GetAsset() == "USD/2" {
				foundUSD = true
				require.NotNil(t, vol.GetInput(), "USD/2 should have input volume")
				require.NotNil(t, vol.GetOutput(), "USD/2 should have output volume")
			}
		}
		require.True(t, foundUSD, "should have USD/2 in aggregated volumes")

		// GetLedgerStats: verify account and transaction counts
		stats, err := testutil.GetLedgerStats(ctx, client, ledger)
		require.NoError(t, err, "GetLedgerStats failed")
		require.Greater(t, stats.GetAccountCount(), uint64(0), "should have accounts")
		require.Greater(t, stats.GetTransactionCount(), uint64(0), "should have transactions")
		t.Logf("LedgerStats: %d accounts, %d transactions",
			stats.GetAccountCount(), stats.GetTransactionCount())
	})

	// --- Phase 12b: List with Filters + GetLog ---
	t.Run("ListFiltersAndGetLog", func(t *testing.T) {
		// ListAccountsFiltered with address prefix
		customers, err := testutil.ListAccountsFiltered(ctx, client, ledger, 0, "",
			testutil.AddressPrefixFilter("customer:"))
		require.NoError(t, err, "ListAccountsFiltered failed")
		require.Equal(t, numCustomers, len(customers),
			"should have %d customer accounts", numCustomers)

		// ListTransactionsFiltered with pageSize=10
		txPage, err := testutil.ListTransactionsFiltered(ctx, client, ledger, 10, 0, nil)
		require.NoError(t, err, "ListTransactionsFiltered failed")
		require.LessOrEqual(t, len(txPage), 10,
			"page should have at most 10 transactions")
		require.NotEmpty(t, txPage, "page should not be empty")

		// GetLog by sequence: get first log then fetch it individually
		logs, err := testutil.ListAllLogs(ctx, client)
		require.NoError(t, err, "ListAllLogs failed")
		require.NotEmpty(t, logs, "should have logs")
		firstSeq := logs[0].Sequence

		singleLog, err := testutil.GetLog(ctx, client, firstSeq)
		require.NoError(t, err, "GetLog failed")
		require.Equal(t, firstSeq, singleLog.GetSequence(),
			"GetLog sequence should match requested sequence")
	})

	// --- Phase 13: Audit Trail ---
	numExtraReverts := 1 // deposit revert in RevertBalanceChecked
	numTimestampTxs := 3 // 2 backdated + 1 expand volumes
	numExtraTxs := 3     // inline numscript + raw posting + deposit in RevertBalanceChecked + ref deposit + ik deposit
	t.Run("AuditTrail", func(t *testing.T) {
		checkAuditTrail(t, ctx, client, []auditExpectation{{
			ledger:           ledger,
			minTransactions:  numCustomers + numPurchases + numReverts + numExtraReverts + numExtraTxs + numTimestampTxs,
			expectedReverted: numReverts + numExtraReverts,
		}})
	})

	// --- Tail phases: StoreCheck, Backup, Restart+Verify, BackupRestore+Verify ---
	runPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		checkDoubleEntryBalance(t, ctx, client, ledger)
		checkNoNegativeBalances(t, ctx, client, ledger, []string{"world"})
		checkAccountBalance(t, ctx, client, ledger, "platform:fees", "USD/2", totalFees)

		for i := 1; i <= numCustomers; i++ {
			checkAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("customer:%d", i), "USD/2", customerBalance[i])
		}
		for i := 1; i <= numMerchants; i++ {
			checkAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("merchant:%d", i), "USD/2", big.NewInt(0))
		}

		// BUG: Idempotency replay creates duplicate transaction after restart.
		// After restart/restore, the FSM replays the idempotent replay Raft entry
		// as a NEW transaction because its preloaded data doesn't contain the
		// original idempotency key. This adds +200 to customer:1.
		checkAccountBalance(t, ctx, client, ledger,
			"customer:1", "USD/2", expectedCustomer1BeforeRestart)

		checkAuditTrail(t, ctx, client, []auditExpectation{{
			ledger:           ledger,
			minTransactions:  numCustomers + numPurchases + numReverts + numExtraReverts + numExtraTxs + numTimestampTxs,
			expectedReverted: numReverts + numExtraReverts,
		}})
	})
}
