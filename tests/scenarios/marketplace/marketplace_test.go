//go:build scenario

package marketplace

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

// TestMarketplaceLifecycle models a high-volume e-commerce marketplace:
// 50 customers, 10 merchants, 200 purchases with fees, periodic closes,
// reverts, cold-account reads, and merchant payouts.
// Generates ~270 Apply calls to trigger ~5 cache rotations (threshold=50).
func TestMarketplaceLifecycle(t *testing.T) {
	const (
		ledger       = scenario.MarketplaceLedger
		numCustomers = 50
		numMerchants = 10
		numPurchases = 200
		numReverts   = 10
		depositAmt   = 1_000_000 // USD/2 cents — large enough for all purchases
		feePercent   = 3
	)

	sc := scenariotest.SetupSingleNode(t, scenariotest.HTTPPort, scenariotest.GRPCPort)
	ctx, client := sc.Ctx(), sc.Client

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
		scenariotest.ApplyActions(t, ctx, client, scenario.MarketplaceSetupActions()...)
	})

	// --- Phase 1a: Verify Setup via Reads ---
	t.Run("VerifySetup", func(t *testing.T) {
		// GetLedger: verify config and account types
		ledgerInfo, err := actions.GetLedger(ctx, client, ledger)
		require.NoError(t, err)
		require.Equal(t, ledger, ledgerInfo.GetName())
		require.Len(t, ledgerInfo.GetAccountTypes(), 4, "should have 4 account types after setup")

		// ListNumscripts + GetNumscript: verify 3 scripts registered
		scripts, err := actions.ListNumscripts(ctx, client, ledger)
		require.NoError(t, err)
		require.Len(t, scripts, 3, "should have 3 numscripts (deposit, purchase, payout)")

		for _, name := range []string{"deposit", "purchase", "payout"} {
			info, err := actions.GetNumscript(ctx, client, ledger, name, "1.0.0")
			require.NoError(t, err, "GetNumscript(%s) failed", name)
			require.Equal(t, name, info.GetName())
			require.Equal(t, "1.0.0", info.GetVersion())
			require.NotEmpty(t, info.GetContent())
		}
	})

	// --- Phase 1b: Account Type Lifecycle (Add/Update/Remove) ---
	t.Run("AccountTypeLifecycle", func(t *testing.T) {
		// Verify account types are present after setup
		ledgers, err := actions.ListLedgers(ctx, client)
		require.NoError(t, err)
		ledgerInfo := ledgers[ledger]
		require.NotNil(t, ledgerInfo, "ledger %q should exist", ledger)
		require.Len(t, ledgerInfo.GetAccountTypes(), 4, "should have 4 account types")

		// Add a temporary type
		scenariotest.ApplyActions(t, ctx, client,
			actions.AddAccountTypeAction(ledger, "temp-type", "temp:{id}"),
		)

		// Verify it was added
		ledgers, err = actions.ListLedgers(ctx, client)
		require.NoError(t, err)
		require.Len(t, ledgers[ledger].GetAccountTypes(), 5, "should have 5 account types after add")

		// Remove the type
		scenariotest.ApplyActions(t, ctx, client,
			actions.RemoveAccountTypeAction(ledger, "temp-type"),
		)

		// Verify removal
		ledgers, err = actions.ListLedgers(ctx, client)
		require.NoError(t, err)
		require.Len(t, ledgers[ledger].GetAccountTypes(), 4, "should have 4 account types after remove")

		// Account type violation: using an address that doesn't match any registered type
		// should fail when enforcement is STRICT
		violationErr := scenariotest.ApplyActionsExpectError(ctx, client,
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", "unknown:address", big.NewInt(100), "USD/2"),
			}, nil, nil),
		)
		require.Error(t, violationErr, "expected account type violation for unknown:address")
	})

	// --- Phase 2: Customer Deposits (50 Apply calls) ---
	t.Run("CustomerDeposits", func(t *testing.T) {
		reqs := make([]*servicepb.Request, 0, numCustomers)
		for i := 1; i <= numCustomers; i++ {
			reqs = append(reqs, actions.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
				"customer": fmt.Sprintf("customer:%d", i),
				"amount":   fmt.Sprintf("USD/2 %d", depositAmt),
			}, nil))
		}
		scenariotest.ApplyActions(t, ctx, client, reqs...)
	})

	// --- Phase 3: Purchases with Fees (200 iterations, with periodic closes and reads) ---
	t.Run("PurchasesWithFees", func(t *testing.T) {
		for i := 0; i < numPurchases; i++ {
			customer := 1 + i%numCustomers
			merchant := 1 + i%numMerchants
			amount := int64(1000 + i*100)

			resp := scenariotest.ApplyActions(t, ctx, client,
				actions.CreateScriptRefTransactionAction(ledger, "purchase", "1.0.0", map[string]string{
					"customer": fmt.Sprintf("customer:%d", customer),
					"merchant": fmt.Sprintf("merchant:%d", merchant),
					"amount":   fmt.Sprintf("USD/2 %d", amount),
				}, nil),
			)
			txID := scenariotest.GetCreatedTransactionID(t, resp)
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
				scenariotest.ClosePeriodAndWait(t, ctx, client, "period close timed out at purchase %d", i)
				scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
			}

			// Every 20 transactions: read a "cold" account (not recently touched)
			if (i+1)%20 == 0 {
				coldCustomer := 1 + (i+numCustomers/2)%numCustomers
				_, err := actions.GetAccount(ctx, client, ledger, fmt.Sprintf("customer:%d", coldCustomer))
				require.NoError(t, err, "failed to read cold account customer:%d", coldCustomer)
			}
		}

		// Spot-check platform fees
		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "platform:fees", "USD/2", totalFees)

		// GetTransaction: verify a purchase transaction has correct structure
		txResp, err := actions.GetTransaction(ctx, client, ledger, purchaseTxIDs[0])
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

		scenariotest.ApplyActions(t, ctx, client,
			actions.WithTimestamp(
				actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting("world", "platform:payouts", big.NewInt(100), "USD/2"),
				}, map[string]string{"backdated": "true"}),
				pastTime1,
			),
		)

		scenariotest.ApplyActions(t, ctx, client,
			actions.WithTimestamp(
				actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting("world", "platform:payouts", big.NewInt(200), "USD/2"),
				}, map[string]string{"backdated": "true"}),
				pastTime2,
			),
		)

		// WithExpandVolumes: verify the response contains volumes
		expandResp := scenariotest.ApplyActions(t, ctx, client,
			actions.WithExpandVolumes(
				actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting("world", "platform:payouts", big.NewInt(50), "USD/2"),
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

			scenariotest.ApplyActions(t, ctx, client,
				actions.RevertTransactionAction(ledger, purchaseTxIDs[idx], true, false, nil),
			)
			purchaseRecords[idx].reverted = true

			fee := p.amount * feePercent / 100
			net := p.amount - fee
			customerBalance[p.customer].Add(customerBalance[p.customer], big.NewInt(p.amount))
			merchantBalance[p.merchant].Sub(merchantBalance[p.merchant], big.NewInt(net))
			totalFees.Sub(totalFees, big.NewInt(fee))
		}

		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "platform:fees", "USD/2", totalFees)
	})

	// --- Phase 5: Final Period Close ---
	t.Run("FinalPeriodClose", func(t *testing.T) {
		scenariotest.ClosePeriodAndWait(t, ctx, client, "final period close timed out")
	})

	// --- Phase 6: Merchant Payouts ---
	t.Run("MerchantPayouts", func(t *testing.T) {
		for i := 1; i <= numMerchants; i++ {
			bal := merchantBalance[i]
			if bal.Sign() <= 0 {
				continue
			}
			scenariotest.ApplyActions(t, ctx, client,
				actions.CreateScriptRefTransactionAction(ledger, "payout", "1.0.0", map[string]string{
					"merchant": fmt.Sprintf("merchant:%d", i),
					"amount":   fmt.Sprintf("USD/2 %d", bal.Int64()),
				}, nil),
			)
		}

		for i := 1; i <= numMerchants; i++ {
			scenariotest.CheckAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("merchant:%d", i), "USD/2", big.NewInt(0))
		}
	})

	// --- Phase 7: Metadata Operations ---
	t.Run("MetadataOperations", func(t *testing.T) {
		// Add account metadata
		scenariotest.ApplyActions(t, ctx, client,
			actions.SaveAccountMetadataAction(ledger, "customer:1", map[string]string{
				"tier": "gold",
				"kyc":  "verified",
			}),
			actions.SaveAccountMetadataAction(ledger, "merchant:1", map[string]string{
				"category": "electronics",
			}),
		)

		// Verify account metadata was stored
		acct, err := actions.GetAccount(ctx, client, ledger, "customer:1")
		require.NoError(t, err)
		tier := actions.FindMetadataValue(acct.Metadata, "tier")
		require.NotNil(t, tier, "tier metadata should exist")

		// Add transaction metadata
		scenariotest.ApplyActions(t, ctx, client,
			actions.SaveTransactionMetadataAction(ledger, purchaseTxIDs[0], map[string]string{
				"flagged": "true",
				"reason":  "review",
			}),
		)

		// Delete account metadata
		scenariotest.ApplyActions(t, ctx, client,
			actions.DeleteAccountMetadataAction(ledger, "customer:1", "kyc"),
		)
		acct, err = actions.GetAccount(ctx, client, ledger, "customer:1")
		require.NoError(t, err)
		require.Nil(t, actions.FindMetadataValue(acct.Metadata, "kyc"), "kyc should be deleted")
		require.NotNil(t, actions.FindMetadataValue(acct.Metadata, "tier"), "tier should remain")

		// Delete transaction metadata
		scenariotest.ApplyActions(t, ctx, client,
			actions.DeleteTransactionMetadataAction(ledger, purchaseTxIDs[0], "reason"),
		)
	})

	// --- Phase 8: Inline Numscript & Raw Postings ---
	t.Run("InlineNumscriptAndRawPostings", func(t *testing.T) {
		// Inline Numscript (not ScriptReference)
		scenariotest.ApplyActions(t, ctx, client,
			actions.CreateScriptTransactionAction(ledger, `vars {
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
		scenariotest.ApplyActions(t, ctx, client,
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("customer:2", "customer:3", big.NewInt(50), "USD/2"),
			}, nil, nil),
		)
		customerBalance[2].Sub(customerBalance[2], big.NewInt(50))
		customerBalance[3].Add(customerBalance[3], big.NewInt(50))

		// Raw postings insufficient funds — should fail
		err := scenariotest.ApplyActionsExpectError(ctx, client,
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("customer:50", "customer:49", big.NewInt(999_999_999), "USD/2"),
			}, nil, nil),
		)
		require.Error(t, err, "expected insufficient funds error for raw posting")
	})

	// --- Phase 9: Revert (force=false, balance-checked) ---
	t.Run("RevertBalanceChecked", func(t *testing.T) {
		// Create a small transaction, then revert it with force=false.
		// This should succeed because customer:3 has enough balance.
		resp := scenariotest.ApplyActions(t, ctx, client,
			actions.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
				"customer": "customer:3",
				"amount":   "USD/2 500",
			}, nil),
		)
		depositTxID := scenariotest.GetCreatedTransactionID(t, resp)
		customerBalance[3].Add(customerBalance[3], big.NewInt(500))

		// Revert with force=false — world will receive back, no balance issue
		scenariotest.ApplyActions(t, ctx, client,
			actions.RevertTransactionAction(ledger, depositTxID, false, false, nil),
		)
		customerBalance[3].Sub(customerBalance[3], big.NewInt(500))

		// Try to revert the same transaction again — should fail (already reverted)
		err := scenariotest.ApplyActionsExpectError(ctx, client,
			actions.RevertTransactionAction(ledger, depositTxID, false, false, nil),
		)
		require.Error(t, err, "expected already-reverted error")
	})

	// --- Phase 10: Idempotency & References ---
	t.Run("IdempotencyAndReferences", func(t *testing.T) {
		// Transaction with a reference
		refAction := actions.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:1",
			"amount":   "USD/2 100",
		}, nil)
		refAction.GetApply().GetAction().GetCreateTransaction().Reference = "unique-ref-001"
		scenariotest.ApplyActions(t, ctx, client, refAction)
		customerBalance[1].Add(customerBalance[1], big.NewInt(100))

		// Duplicate reference — should fail
		refAction2 := actions.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:2",
			"amount":   "USD/2 100",
		}, nil)
		refAction2.GetApply().GetAction().GetCreateTransaction().Reference = "unique-ref-001"
		err := scenariotest.ApplyActionsExpectError(ctx, client, refAction2)
		require.Error(t, err, "expected reference conflict error")

		// Idempotency: create a transaction with an idempotency key
		ikAction := actions.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:1",
			"amount":   "USD/2 200",
		}, nil)
		ikAction.IdempotencyKey = "ik-deposit-001"
		scenariotest.ApplyActions(t, ctx, client, ikAction)
		customerBalance[1].Add(customerBalance[1], big.NewInt(200))

		// Idempotency replay: same key + same content → should succeed (return original result)
		ikReplay := actions.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:1",
			"amount":   "USD/2 200",
		}, nil)
		ikReplay.IdempotencyKey = "ik-deposit-001"
		scenariotest.ApplyActions(t, ctx, client, ikReplay)
		// No balance change — idempotent replay returns the original log

		// Idempotency conflict: same key + different content
		ikConflict := actions.CreateScriptRefTransactionAction(ledger, "deposit", "1.0.0", map[string]string{
			"customer": "customer:2",
			"amount":   "USD/2 999",
		}, nil)
		ikConflict.IdempotencyKey = "ik-deposit-001"
		err = scenariotest.ApplyActionsExpectError(ctx, client, ikConflict)
		require.Error(t, err, "expected idempotency key conflict")
	})

	// Capture expected balance for customer:1 before restart to verify
	// idempotency key survives snapshot restore (regression test).
	expectedCustomer1BeforeRestart := new(big.Int).Set(customerBalance[1])

	// --- Phase 11: DeleteNumscript ---
	t.Run("DeleteNumscript", func(t *testing.T) {
		// Save a temporary script, then delete it
		scenariotest.ApplyActions(t, ctx, client,
			actions.SaveNumscriptWithVersionAction(ledger, "temp_script", `vars {
  monetary $amount
}
send $amount (
  source = @world
  destination = @customer:1
)`, "1.0.0"),
		)
		scenariotest.ApplyActions(t, ctx, client,
			actions.DeleteNumscriptAction(ledger, "temp_script"),
		)
	})

	// --- Phase 11b: Prepared Queries (created by shared scenario) ---
	t.Run("PreparedQueries", func(t *testing.T) {
		// 1. Hardcoded filter — verify customer-query exists and works
		queries, err := actions.ListPreparedQueries(ctx, client, ledger)
		require.NoError(t, err, "ListPreparedQueries failed")
		require.GreaterOrEqual(t, len(queries), 1, "should have at least 1 prepared query")
		var found bool
		for _, q := range queries {
			if q.GetName() == "customer-query" {
				found = true
			}
		}
		require.True(t, found, "customer-query should be in the list")

		execResp, err := actions.ExecutePreparedQuery(ctx, client, ledger, "customer-query",
			commonpb.QueryMode_QUERY_MODE_LIST, 10)
		require.NoError(t, err, "ExecutePreparedQuery failed")
		require.NotNil(t, execResp, "execute response should not be nil")

		// 2. Parameterized address prefix — reusable query, different prefixes at runtime
		// Execute with prefix=customer: → should return all customers
		resp, err := actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100, map[string]*commonpb.ParameterValue{"prefix": actions.StringParam("customer:")})
		require.NoError(t, err, "ExecutePreparedQueryWithParams(customer:) failed")
		cursor := resp.GetCursor()
		require.NotNil(t, cursor, "expected cursor result")
		require.Equal(t, numCustomers, len(cursor.GetAccountData()),
			"parameterized query with prefix=customer: should return %d accounts", numCustomers)

		// Execute same query with prefix=merchant: → should return all merchants
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100, map[string]*commonpb.ParameterValue{"prefix": actions.StringParam("merchant:")})
		require.NoError(t, err, "ExecutePreparedQueryWithParams(merchant:) failed")
		cursor = resp.GetCursor()
		require.NotNil(t, cursor, "expected cursor result for merchants")
		require.Equal(t, numMerchants, len(cursor.GetAccountData()),
			"parameterized query with prefix=merchant: should return %d accounts", numMerchants)

		// Execute with prefix=platform: → should return platform accounts
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100, map[string]*commonpb.ParameterValue{"prefix": actions.StringParam("platform:")})
		require.NoError(t, err, "ExecutePreparedQueryWithParams(platform:) failed")
		cursor = resp.GetCursor()
		require.NotNil(t, cursor, "expected cursor result for platform")
		require.GreaterOrEqual(t, len(cursor.GetAccountData()), 1,
			"parameterized query with prefix=platform: should return at least 1 account")

		// 3. Update the hardcoded query, then delete both
		err = actions.UpdatePreparedQuery(ctx, client, ledger, "customer-query",
			actions.AddressPrefixFilter("merchant:"),
		)
		require.NoError(t, err, "UpdatePreparedQuery failed")

		err = actions.DeletePreparedQuery(ctx, client, ledger, "customer-query")
		require.NoError(t, err, "DeletePreparedQuery(customer-query) failed")
		err = actions.DeletePreparedQuery(ctx, client, ledger, "accounts-by-prefix")
		require.NoError(t, err, "DeletePreparedQuery(accounts-by-prefix) failed")

		// Verify both are gone
		queries, err = actions.ListPreparedQueries(ctx, client, ledger)
		require.NoError(t, err, "ListPreparedQueries after delete failed")
		for _, q := range queries {
			require.NotEqual(t, "customer-query", q.GetName(), "customer-query should be deleted")
			require.NotEqual(t, "accounts-by-prefix", q.GetName(), "accounts-by-prefix should be deleted")
		}
	})

	// --- Phase 12: Final Invariants ---
	t.Run("FinalInvariants", func(t *testing.T) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})
		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "platform:fees", "USD/2", totalFees)

		for i := 1; i <= numCustomers; i++ {
			scenariotest.CheckAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("customer:%d", i), "USD/2", customerBalance[i])
		}

		// AggregateVolumes: verify USD/2 is present
		aggResult, err := actions.AggregateVolumes(ctx, client, ledger)
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
		stats, err := actions.GetLedgerStats(ctx, client, ledger)
		require.NoError(t, err, "GetLedgerStats failed")
		require.Greater(t, stats.GetVolumeCount(), uint64(0), "should have volumes")
		require.Greater(t, stats.GetTransactionCount(), uint64(0), "should have transactions")
		t.Logf("LedgerStats: %d volumes, %d transactions",
			stats.GetVolumeCount(), stats.GetTransactionCount())
	})

	// --- Phase 12b: List with Filters + GetLog ---
	t.Run("ListFiltersAndGetLog", func(t *testing.T) {
		// ListAccountsFiltered with address prefix
		customers, err := actions.ListAccountsFiltered(ctx, client, ledger, 0, "",
			actions.AddressPrefixFilter("customer:"))
		require.NoError(t, err, "ListAccountsFiltered failed")
		require.Equal(t, numCustomers, len(customers),
			"should have %d customer accounts", numCustomers)

		// ListTransactionsFiltered with pageSize=10
		txPage, err := actions.ListTransactionsFiltered(ctx, client, ledger, 10, 0, nil)
		require.NoError(t, err, "ListTransactionsFiltered failed")
		require.LessOrEqual(t, len(txPage), 10,
			"page should have at most 10 transactions")
		require.NotEmpty(t, txPage, "page should not be empty")

		// GetLog by sequence: get first log then fetch it individually
		logs, err := actions.ListAllLogs(ctx, client, ledger)
		require.NoError(t, err, "ListAllLogs failed")
		require.NotEmpty(t, logs, "should have logs")
		firstSeq := logs[0].Sequence

		singleLog, err := actions.GetLog(ctx, client, firstSeq)
		require.NoError(t, err, "GetLog failed")
		require.Equal(t, firstSeq, singleLog.GetSequence(),
			"GetLog sequence should match requested sequence")
	})

	// --- Phase 13: Audit Trail ---
	numExtraReverts := 1 // deposit revert in RevertBalanceChecked
	numTimestampTxs := 3 // 2 backdated + 1 expand volumes
	numExtraTxs := 3     // inline numscript + raw posting + deposit in RevertBalanceChecked + ref deposit + ik deposit
	t.Run("AuditTrail", func(t *testing.T) {
		scenariotest.CheckAuditTrail(t, ctx, client, []scenariotest.AuditExpectation{{
			Ledger:           ledger,
			MinTransactions:  numCustomers + numPurchases + numReverts + numExtraReverts + numExtraTxs + numTimestampTxs,
			ExpectedReverted: numReverts + numExtraReverts,
		}})
	})

	// --- Tail phases: StoreCheck, Backup, Restart+Verify, BackupRestore+Verify ---
	scenariotest.RunPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})
		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "platform:fees", "USD/2", totalFees)

		for i := 1; i <= numCustomers; i++ {
			scenariotest.CheckAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("customer:%d", i), "USD/2", customerBalance[i])
		}
		for i := 1; i <= numMerchants; i++ {
			scenariotest.CheckAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("merchant:%d", i), "USD/2", big.NewInt(0))
		}

		// Regression: idempotency key must survive snapshot restore.
		scenariotest.CheckAccountBalance(t, ctx, client, ledger,
			"customer:1", "USD/2", expectedCustomer1BeforeRestart)

		scenariotest.CheckAuditTrail(t, ctx, client, []scenariotest.AuditExpectation{{
			Ledger:           ledger,
			MinTransactions:  numCustomers + numPurchases + numReverts + numExtraReverts + numExtraTxs + numTimestampTxs,
			ExpectedReverted: numReverts + numExtraReverts,
		}})
	})
}
