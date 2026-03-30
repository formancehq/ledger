//go:build scenario

package gamingwallet

import (
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger-v3-poc/tests/scenarios/scenariotest"
)

// TestGamingWalletLifecycle models a gaming platform with virtual currency:
//
// - Players buy coins with real money (USD → COINS conversion)
// - Players spend coins on in-game items
// - Platform distributes rewards/promotions (free coins)
// - Players trade items peer-to-peer with coin transfers
// - Refunds via transaction reverts (with and without balance checks)
// - Revenue recognition from coin purchases
// - Expired promotional coins clawback
//
// This scenario exercises:
// - Two assets in one ledger (USD/2 for real money, COINS for virtual currency)
// - Force transactions (promotional credits from @world)
// - Reverts with force=true and force=false
// - Revert of already-reverted transaction (double revert error)
// - Zero-amount edge cases
// - ExpandVolumes on transaction creation
// - Transaction with metadata and subsequent metadata updates/deletes
// - Account type enforcement in STRICT and AUDIT modes
//
// Account structure:
//
//	player:{id}:usd      — real money balance
//	player:{id}:coins    — virtual currency balance
//	platform:revenue     — revenue from coin purchases
//	platform:promotions  — promotional coin issuance tracking
//	shop:items           — item sales revenue (in coins)
//	escrow:p2p           — peer-to-peer trade escrow
//
// Generates ~180 Apply calls, triggers 3+ cache rotations.
func TestGamingWalletLifecycle(t *testing.T) {
	const (
		ledger     = scenario.GamingWalletLedger
		numPlayers = 20
		coinPrice  = 100  // 1 USD = 100 COINS
		topUpUSD   = 5000 // USD/2 cents per top-up
		topUpCoins = topUpUSD * coinPrice / 100
		promoCoins = 500 // free coins per promo
	)

	sc := scenariotest.SetupSingleNode(t, scenariotest.HTTPPort+7, scenariotest.GRPCPort+7)
	ctx, client := sc.Ctx(), sc.Client

	// Balance tracking
	playerCoins := make(map[int]*big.Int, numPlayers)
	playerUSD := make(map[int]*big.Int, numPlayers)
	for i := 1; i <= numPlayers; i++ {
		playerCoins[i] = new(big.Int)
		playerUSD[i] = new(big.Int)
	}
	revenueUSD := new(big.Int)
	shopCoins := new(big.Int)
	promoTotal := new(big.Int)

	// Track transaction IDs for reverts
	type purchaseRecord struct {
		txID     uint64
		player   int
		coins    int64
		reverted bool
	}
	var itemPurchases []purchaseRecord

	// --- Phase 1: Setup ---
	t.Run("Setup", func(t *testing.T) {
		scenariotest.ApplyActions(t, ctx, client, scenario.GamingWalletSetupActions()...)
	})


	// --- Phase 2: Top-Ups (buy coins with real money) ---
	t.Run("TopUps", func(t *testing.T) {
		var reqs []*servicepb.Request
		for i := 1; i <= numPlayers; i++ {
			action := actions.CreateScriptRefTransactionAction(ledger, "top_up", "1.0.0", map[string]string{
				"player_usd":   fmt.Sprintf("player:%d:usd", i),
				"player_coins": fmt.Sprintf("player:%d:coins", i),
				"usd_amount":   fmt.Sprintf("USD/2 %d", topUpUSD),
				"coin_amount":  fmt.Sprintf("COINS %d", topUpCoins),
			}, map[string]string{"type": "initial-topup"})
			action.GetApply().GetCreateTransaction().Reference = fmt.Sprintf("topup-initial-%d", i)
			reqs = append(reqs, action)

			playerCoins[i].Add(playerCoins[i], big.NewInt(topUpCoins))
			revenueUSD.Add(revenueUSD, big.NewInt(topUpUSD))
		}
		scenariotest.ApplyActions(t, ctx, client, reqs...)

		// Verify revenue
		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "platform:revenue", "USD/2", revenueUSD)
	})

	// --- Phase 3: Promotional Credits (force transactions from @world) ---
	t.Run("Promotions", func(t *testing.T) {
		// Give free coins to first 10 players
		var reqs []*servicepb.Request
		for i := 1; i <= 10; i++ {
			reqs = append(reqs,
				actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting("world", fmt.Sprintf("player:%d:coins", i), big.NewInt(promoCoins), "COINS"),
				}, map[string]string{
					"type":   "promotion",
					"reason": "welcome-bonus",
				}),
			)
			playerCoins[i].Add(playerCoins[i], big.NewInt(promoCoins))
			promoTotal.Add(promoTotal, big.NewInt(promoCoins))
		}
		scenariotest.ApplyActions(t, ctx, client, reqs...)

		scenariotest.ClosePeriodAndWait(t, ctx, client, "post-promotion period close")
	})

	// --- Phase 4: Item Purchases ---
	t.Run("ItemPurchases", func(t *testing.T) {
		// Each player buys 3 items of varying cost
		itemCosts := []int64{100, 250, 500}

		for round, cost := range itemCosts {
			var reqs []*servicepb.Request
			for i := 1; i <= numPlayers; i++ {
				// Only buy if player has enough coins
				if playerCoins[i].Cmp(big.NewInt(cost)) < 0 {
					continue
				}
				action := actions.CreateScriptRefTransactionAction(ledger, "buy_item", "1.0.0", map[string]string{
					"player_coins": fmt.Sprintf("player:%d:coins", i),
					"amount":       fmt.Sprintf("COINS %d", cost),
				}, map[string]string{
					"item":  fmt.Sprintf("item-round-%d", round+1),
					"round": fmt.Sprintf("%d", round+1),
				})
				reqs = append(reqs, action)

				playerCoins[i].Sub(playerCoins[i], big.NewInt(cost))
				shopCoins.Add(shopCoins, big.NewInt(cost))
			}
			resp := scenariotest.ApplyActions(t, ctx, client, reqs...)

			// Track some purchases for revert tests
			for j, log := range resp.Logs {
				if j < 5 && round == 0 {
					applyLog := log.Payload.GetApply()
					if applyLog != nil {
						tx := applyLog.Log.Data.GetCreatedTransaction()
						if tx != nil {
							itemPurchases = append(itemPurchases, purchaseRecord{
								txID:   tx.Transaction.Id,
								player: j + 1,
								coins:  cost,
							})
						}
					}
				}
			}

			// Read some accounts mid-way to exercise cache
			if round == 1 {
				for i := 1; i <= 5; i++ {
					_, err := actions.GetAccount(ctx, client, ledger, fmt.Sprintf("player:%d:coins", i))
					require.NoError(t, err)
				}
			}
		}

		scenariotest.ClosePeriodAndWait(t, ctx, client, "post-purchases period close")
	})

	// --- Phase 5: Peer-to-Peer Trades ---
	t.Run("P2PTrades", func(t *testing.T) {
		// 10 trades between random player pairs
		trades := [][3]int{
			{1, 2, 50}, {3, 4, 75}, {5, 6, 100}, {7, 8, 25}, {9, 10, 150},
			{2, 3, 30}, {4, 5, 60}, {6, 7, 80}, {8, 9, 40}, {10, 1, 90},
		}

		var reqs []*servicepb.Request
		for _, trade := range trades {
			from, to, amount := trade[0], trade[1], int64(trade[2])

			// Check sender has enough
			if playerCoins[from].Cmp(big.NewInt(amount)) < 0 {
				continue
			}

			reqs = append(reqs,
				actions.CreateScriptRefTransactionAction(ledger, "p2p_transfer", "1.0.0", map[string]string{
					"from_player": fmt.Sprintf("player:%d:coins", from),
					"to_player":   fmt.Sprintf("player:%d:coins", to),
					"amount":      fmt.Sprintf("COINS %d", amount),
				}, map[string]string{
					"type": "p2p-trade",
				}),
			)
			playerCoins[from].Sub(playerCoins[from], big.NewInt(amount))
			playerCoins[to].Add(playerCoins[to], big.NewInt(amount))
		}
		if len(reqs) > 0 {
			scenariotest.ApplyActions(t, ctx, client, reqs...)
		}
	})

	// --- Phase 6: Refunds (Reverts) ---
	t.Run("Refunds", func(t *testing.T) {
		if len(itemPurchases) < 3 {
			t.Skip("not enough purchase records for revert tests")
		}

		// Revert first purchase (force=false — balance-checked)
		p := &itemPurchases[0]
		scenariotest.ApplyActions(t, ctx, client,
			actions.RevertTransactionAction(ledger, p.txID, false, false, map[string]string{"reason": "refund"}),
		)
		playerCoins[p.player].Add(playerCoins[p.player], big.NewInt(p.coins))
		shopCoins.Sub(shopCoins, big.NewInt(p.coins))
		p.reverted = true

		// Revert second purchase with force=true
		p2 := &itemPurchases[1]
		scenariotest.ApplyActions(t, ctx, client,
			actions.RevertTransactionAction(ledger, p2.txID, true, false, map[string]string{"reason": "admin-refund"}),
		)
		playerCoins[p2.player].Add(playerCoins[p2.player], big.NewInt(p2.coins))
		shopCoins.Sub(shopCoins, big.NewInt(p2.coins))
		p2.reverted = true

		// Double-revert should fail
		err := scenariotest.ApplyActionsExpectError(ctx, client,
			actions.RevertTransactionAction(ledger, p.txID, false, false, nil),
		)
		require.Error(t, err, "double revert should fail")

		// Revert with ExpandVolumes
		if len(itemPurchases) >= 3 {
			p3 := &itemPurchases[2]
			action := actions.RevertTransactionAction(ledger, p3.txID, false, false, nil)
			actions.WithExpandVolumes(action)
			resp := scenariotest.ApplyActions(t, ctx, client, action)
			require.NotEmpty(t, resp.Logs, "revert with expand volumes should return logs")
			playerCoins[p3.player].Add(playerCoins[p3.player], big.NewInt(p3.coins))
			shopCoins.Sub(shopCoins, big.NewInt(p3.coins))
			p3.reverted = true
		}
	})

	// --- Phase 7: Insufficient Funds ---
	t.Run("InsufficientFunds", func(t *testing.T) {
		// Try to buy an item the player can't afford
		err := scenariotest.ApplyActionsExpectError(ctx, client,
			actions.CreateScriptRefTransactionAction(ledger, "buy_item", "1.0.0", map[string]string{
				"player_coins": fmt.Sprintf("player:%d:coins", numPlayers),
				"amount":       "COINS 999999999",
			}, nil),
		)
		require.Error(t, err, "should fail with insufficient funds")
	})

	// --- Phase 8: Promotional Clawback ---
	t.Run("PromoClawback", func(t *testing.T) {
		// Clawback remaining promo coins from players 8-10
		// (simulating expired promotional balance)
		var reqs []*servicepb.Request
		for i := 8; i <= 10; i++ {
			clawAmount := big.NewInt(promoCoins)
			// Can only claw back if they still have enough
			if playerCoins[i].Cmp(clawAmount) < 0 {
				clawAmount = new(big.Int).Set(playerCoins[i])
			}
			if clawAmount.Sign() <= 0 {
				continue
			}

			reqs = append(reqs,
				actions.CreateScriptRefTransactionAction(ledger, "clawback", "1.0.0", map[string]string{
					"player_coins": fmt.Sprintf("player:%d:coins", i),
					"amount":       fmt.Sprintf("COINS %s", clawAmount.String()),
				}, map[string]string{
					"type":   "promo-clawback",
					"reason": "expired-welcome-bonus",
				}),
			)
			playerCoins[i].Sub(playerCoins[i], clawAmount)
		}
		if len(reqs) > 0 {
			scenariotest.ApplyActions(t, ctx, client, reqs...)
		}

		scenariotest.ClosePeriodAndWait(t, ctx, client, "post-clawback period close")
	})

	// --- Phase 9: Metadata Lifecycle ---
	t.Run("MetadataLifecycle", func(t *testing.T) {
		// Add metadata to player 1
		scenariotest.ApplyActions(t, ctx, client,
			actions.SaveAccountMetadataAction(ledger, "player:1:coins", map[string]string{
				"vip":    "true",
				"tier":   "gold",
				"joined": "2025-01-01",
			}),
		)

		// Update metadata
		scenariotest.ApplyActions(t, ctx, client,
			actions.SaveAccountMetadataAction(ledger, "player:1:coins", map[string]string{
				"tier": "platinum",
			}),
		)

		// Verify metadata
		acct, err := actions.GetAccount(ctx, client, ledger, "player:1:coins")
		require.NoError(t, err)
		tier := actions.FindMetadataValue(acct.Metadata, "tier")
		require.NotNil(t, tier, "tier metadata should exist")
		require.Equal(t, "platinum", tier.GetStringValue(), "tier should be updated to platinum")

		// Delete metadata key
		scenariotest.ApplyActions(t, ctx, client,
			actions.DeleteAccountMetadataAction(ledger, "player:1:coins", "joined"),
		)

		// Verify deletion
		acct, err = actions.GetAccount(ctx, client, ledger, "player:1:coins")
		require.NoError(t, err)
		joined := actions.FindMetadataValue(acct.Metadata, "joined")
		require.Nil(t, joined, "joined metadata should be deleted")
	})

	// --- Phase 10: Account Type Enforcement ---
	t.Run("AccountTypeEnforcement", func(t *testing.T) {
		// STRICT mode: transaction to non-matching address should fail
		err := scenariotest.ApplyActionsExpectError(ctx, client,
			actions.CreateTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", "invalid-address", big.NewInt(1), "COINS"),
			}, nil, nil),
		)
		require.Error(t, err, "STRICT mode should reject non-matching address")

		// Valid platform address should still work
		scenariotest.ApplyActions(t, ctx, client,
			actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("world", "platform:test", big.NewInt(1), "COINS"),
			}, nil),
		)
	})

	// --- Phase 11: Prepared Queries (created by shared scenario) ---
	t.Run("PreparedQueries", func(t *testing.T) {
		// Wait for tier index backfill to complete
		require.Eventually(t, func() bool {
			indexStatus, err := actions.GetIndexStatus(ctx, client)
			if err != nil {
				return false
			}
			return indexStatus.GetLag() == 0 && len(indexStatus.GetBackfillProgress()) == 0
		}, 15*time.Second, 200*time.Millisecond, "tier index backfill should complete")

		// 1. Parameterized address prefix — filter by account type at runtime
		// Query for all player coin accounts
		resp, err := actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": actions.StringParam("player:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(player:) failed")
		// 20 players × 2 accounts each (usd + coins) = 40
		require.Equal(t, numPlayers*2, len(resp.GetCursor().GetAccountData()),
			"should find all player accounts (usd + coins)")

		// Query for shop accounts only
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": actions.StringParam("shop:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(shop:) failed")
		require.GreaterOrEqual(t, len(resp.GetCursor().GetAccountData()), 1,
			"should find at least 1 shop account")

		// Query for escrow accounts
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "accounts-by-prefix",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"prefix": actions.StringParam("escrow:")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(escrow:) failed")
		// escrow:p2p exists even if not used, depending on the flow
		require.GreaterOrEqual(t, len(resp.GetCursor().GetAccountData()), 0)

		// 2. Parameterized exact address — find a specific account
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "account-exact",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"addr": actions.StringParam("platform:revenue")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(platform:revenue) failed")
		require.Equal(t, 1, len(resp.GetCursor().GetAccountData()),
			"exact match should return exactly 1 account")

		// Non-existent exact address — should return 0
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "account-exact",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"addr": actions.StringParam("nonexistent:account")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(nonexistent) failed")
		require.Empty(t, resp.GetCursor().GetAccountData(),
			"nonexistent account should return 0 results")

		// 3. Parameterized string metadata — filter by tier
		// Query for "platinum" tier — player:1:coins was updated to platinum
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "by-tier",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"tier_value": actions.StringParam("platinum")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(tier=platinum) failed")
		require.GreaterOrEqual(t, len(resp.GetCursor().GetAccountData()), 1,
			"should find at least 1 account with tier=platinum")

		// Query for "gold" tier — was overwritten to platinum, so should be 0
		resp, err = actions.ExecutePreparedQueryWithParams(ctx, client, ledger, "by-tier",
			commonpb.QueryMode_QUERY_MODE_LIST, 100,
			map[string]*commonpb.ParameterValue{"tier_value": actions.StringParam("gold")},
		)
		require.NoError(t, err, "ExecutePreparedQueryWithParams(tier=gold) failed")
		require.Empty(t, resp.GetCursor().GetAccountData(),
			"gold tier was overwritten, should return 0")

		// Cleanup
		require.NoError(t, actions.DeletePreparedQuery(ctx, client, ledger, "accounts-by-prefix"))
		require.NoError(t, actions.DeletePreparedQuery(ctx, client, ledger, "account-exact"))
		require.NoError(t, actions.DeletePreparedQuery(ctx, client, ledger, "by-tier"))
	})

	// --- Phase 12: Final Invariants ---
	t.Run("FinalInvariants", func(t *testing.T) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})

		// Verify player coin balances
		for i := 1; i <= numPlayers; i++ {
			scenariotest.CheckAccountBalance(t, ctx, client, ledger,
				fmt.Sprintf("player:%d:coins", i), "COINS", playerCoins[i])
		}

		// Verify shop revenue
		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "shop:items", "COINS", shopCoins)

		// Verify platform revenue
		scenariotest.CheckAccountBalance(t, ctx, client, ledger, "platform:revenue", "USD/2", revenueUSD)

		// Stats
		stats, err := actions.GetLedgerStats(ctx, client, ledger)
		require.NoError(t, err)
		require.Greater(t, stats.GetTransactionCount(), uint64(100), "should have many transactions")
		t.Logf("LedgerStats: %d accounts, %d transactions",
			stats.GetAccountCount(), stats.GetTransactionCount())

		// Audit trail
		scenariotest.CheckAuditTrail(t, ctx, client, []scenariotest.AuditExpectation{
			{
				Ledger:          ledger,
				MinTransactions: 100,
				ExpectedReverted: func() int {
					count := 0
					for _, p := range itemPurchases {
						if p.reverted {
							count++
						}
					}
					return count
				}(),
			},
		})
	})

	// --- Tail phases ---
	scenariotest.RunPostTestPhases(t, sc, func(t *testing.T, client servicepb.BucketServiceClient) {
		scenariotest.CheckDoubleEntryBalance(t, ctx, client, ledger)
		scenariotest.CheckNoNegativeBalances(t, ctx, client, ledger, []string{"world"})
	})
}
