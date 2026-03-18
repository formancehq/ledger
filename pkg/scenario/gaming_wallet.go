package scenario

import (
	"fmt"
	"math/big"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario/actions"
)

func init() { Register("gaming-wallet", RunGamingWallet) }

// GamingWalletLedger is the ledger name used by the gaming wallet scenario.
const GamingWalletLedger = "gaming"

// GamingWalletSetupActions returns the Apply requests that create the ledger,
// account types, and numscript library for the gaming wallet scenario.
func GamingWalletSetupActions() []*servicepb.Request {
	return []*servicepb.Request{
		actions.CreateLedgerAction(GamingWalletLedger, nil),
		actions.AddAccountTypeAction(GamingWalletLedger, "player-usd", "player:{id}:usd", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(GamingWalletLedger, "player-coins", "player:{id}:coins", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(GamingWalletLedger, "platform", "platform:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_AUDIT),
		actions.AddAccountTypeAction(GamingWalletLedger, "shop", "shop:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(GamingWalletLedger, "escrow", "escrow:{type}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.SaveNumscriptWithVersionAction(GamingWalletLedger, "top_up", `vars {
  account $player_usd
  account $player_coins
  monetary $usd_amount
  monetary $coin_amount
}
send $usd_amount (
  source = @world
  destination = $player_usd
)
send $usd_amount (
  source = $player_usd
  destination = @platform:revenue
)
send $coin_amount (
  source = @world
  destination = $player_coins
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(GamingWalletLedger, "buy_item", `vars {
  account $player_coins
  monetary $amount
}
send $amount (
  source = $player_coins
  destination = @shop:items
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(GamingWalletLedger, "p2p_transfer", `vars {
  account $from_player
  account $to_player
  monetary $amount
}
send $amount (
  source = $from_player
  destination = $to_player
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(GamingWalletLedger, "clawback", `vars {
  account $player_coins
  monetary $amount
}
send $amount (
  source = $player_coins
  destination = @platform:promotions
)`, "1.0.0"),
		actions.CreateAccountMetadataIndexAction(GamingWalletLedger, "tier"),
		actions.CreatePreparedQueryAction("accounts-by-prefix", GamingWalletLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamAddressPrefixFilter("prefix"),
		),
		actions.CreatePreparedQueryAction("account-exact", GamingWalletLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamAddressExactFilter("addr"),
		),
		actions.CreatePreparedQueryAction("by-tier", GamingWalletLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamStringMetadataFilter("tier", "tier_value"),
		),
	}
}

// RunGamingWallet provisions a gaming platform scenario with virtual currency:
// players buy coins, spend on items, trade peer-to-peer, receive promotions,
// and have expired promo coins clawed back.
func RunGamingWallet(r *Runner) error {
	const (
		numPlayers = 20
		coinPrice  = 100
		topUpUSD   = 5000
		topUpCoins = topUpUSD * coinPrice / 100
		promoCoins = 500
	)

	ledger := GamingWalletLedger

	// Balance tracking (needed for conditional logic)
	playerCoins := make(map[int]*big.Int, numPlayers)
	for i := 1; i <= numPlayers; i++ {
		playerCoins[i] = new(big.Int)
	}

	// --- Setup ---
	if _, err := r.Step("Setup", GamingWalletSetupActions()...); err != nil {
		return err
	}

	// --- Top-Ups ---
	{
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
		}
		if _, err := r.Step("TopUps", reqs...); err != nil {
			return err
		}
	}

	// --- Promotions (free coins to first 10 players) ---
	{
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
		}
		if _, err := r.Step("Promotions", reqs...); err != nil {
			return err
		}
	}

	// --- Item Purchases (3 rounds) ---
	itemCosts := []int64{100, 250, 500}
	for round, cost := range itemCosts {
		var reqs []*servicepb.Request
		for i := 1; i <= numPlayers; i++ {
			if playerCoins[i].Cmp(big.NewInt(cost)) < 0 {
				continue
			}
			action := actions.CreateScriptRefTransactionAction(ledger, "buy_item", "1.0.0", map[string]string{
				"player_coins": fmt.Sprintf("player:%d:coins", i),
				"amount":       fmt.Sprintf("COINS %d", cost),
			}, map[string]string{
				"item":  fmt.Sprintf("item-round-%d", round+1),
				"round": strconv.Itoa(round + 1),
			})
			reqs = append(reqs, action)
			playerCoins[i].Sub(playerCoins[i], big.NewInt(cost))
		}
		if _, err := r.Step(fmt.Sprintf("ItemPurchases/Round%d", round+1), reqs...); err != nil {
			return err
		}
	}

	// --- P2P Trades ---
	trades := [][3]int{
		{1, 2, 50}, {3, 4, 75}, {5, 6, 100}, {7, 8, 25}, {9, 10, 150},
		{2, 3, 30}, {4, 5, 60}, {6, 7, 80}, {8, 9, 40}, {10, 1, 90},
	}
	{
		var reqs []*servicepb.Request
		for _, trade := range trades {
			from, to, amount := trade[0], trade[1], int64(trade[2])
			if playerCoins[from].Cmp(big.NewInt(amount)) < 0 {
				continue
			}
			reqs = append(reqs,
				actions.CreateScriptRefTransactionAction(ledger, "p2p_transfer", "1.0.0", map[string]string{
					"from_player": fmt.Sprintf("player:%d:coins", from),
					"to_player":   fmt.Sprintf("player:%d:coins", to),
					"amount":      fmt.Sprintf("COINS %d", amount),
				}, map[string]string{"type": "p2p-trade"}),
			)
			playerCoins[from].Sub(playerCoins[from], big.NewInt(amount))
			playerCoins[to].Add(playerCoins[to], big.NewInt(amount))
		}
		if len(reqs) > 0 {
			if _, err := r.Step("P2PTrades", reqs...); err != nil {
				return err
			}
		}
	}

	// --- Promo Clawback (players 8-10) ---
	{
		var reqs []*servicepb.Request
		for i := 8; i <= 10; i++ {
			clawAmount := big.NewInt(promoCoins)
			if playerCoins[i].Cmp(clawAmount) < 0 {
				clawAmount = new(big.Int).Set(playerCoins[i])
			}
			if clawAmount.Sign() <= 0 {
				continue
			}
			reqs = append(reqs,
				actions.CreateScriptRefTransactionAction(ledger, "clawback", "1.0.0", map[string]string{
					"player_coins": fmt.Sprintf("player:%d:coins", i),
					"amount":       "COINS " + clawAmount.String(),
				}, map[string]string{
					"type":   "promo-clawback",
					"reason": "expired-welcome-bonus",
				}),
			)
			playerCoins[i].Sub(playerCoins[i], clawAmount)
		}
		if len(reqs) > 0 {
			if _, err := r.Step("PromoClawback", reqs...); err != nil {
				return err
			}
		}
	}

	// --- Metadata Lifecycle ---
	if _, err := r.Step("Metadata/Save",
		actions.SaveAccountMetadataAction(ledger, "player:1:coins", map[string]string{
			"vip":    "true",
			"tier":   "gold",
			"joined": "2025-01-01",
		}),
	); err != nil {
		return err
	}
	if _, err := r.Step("Metadata/Update",
		actions.SaveAccountMetadataAction(ledger, "player:1:coins", map[string]string{
			"tier": "platinum",
		}),
	); err != nil {
		return err
	}
	if _, err := r.Step("Metadata/Delete",
		actions.DeleteAccountMetadataAction(ledger, "player:1:coins", "joined"),
	); err != nil {
		return err
	}

	return nil
}
