package scenario

import (
	"context"
	"fmt"
	"math/big"
	"strconv"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
)

func init() { Register("gaming-wallet", RunGamingWallet) }

const (
	// GamingWalletLedger is the ledger name used by the gaming wallet scenario.
	GamingWalletLedger     = "gaming"
	GamingWalletNumPlayers = 20
)

// GamingWalletBlocks returns the atomic blocks for the gaming wallet scenario.
func GamingWalletBlocks() *BlockGroup {
	return &BlockGroup{
		Setup: GamingWalletSetupActions,
		Blocks: []*Block{
			{Name: "gaming/topup", Run: gamingTopUp},
			{Name: "gaming/buy_item", Run: gamingBuyItem},
			{Name: "gaming/p2p_trade", Run: gamingP2PTrade},
			{Name: "gaming/promotion", Run: gamingPromotion},
			{Name: "gaming/clawback", Run: gamingClawback},
		},
	}
}

func gamingTopUp(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	playerID := 1 + RandIntN(r, GamingWalletNumPlayers)
	usdAmount := int64(1000 + RandIntN(r, 10000))
	coinAmount := usdAmount

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(GamingWalletLedger, "top_up", "1.0.0", map[string]string{
			"player_usd":   fmt.Sprintf("player:%d:usd", playerID),
			"player_coins": fmt.Sprintf("player:%d:coins", playerID),
			"usd_amount":   fmt.Sprintf("USD/2 %d", usdAmount),
			"coin_amount":  fmt.Sprintf("COINS %d", coinAmount),
		}, map[string]string{"type": "topup"}),
	)
}

func gamingBuyItem(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	playerID := 1 + RandIntN(r, GamingWalletNumPlayers)
	playerCoins := fmt.Sprintf("player:%d:coins", playerID)

	bal, ok := GetAccountBalance(ctx, client, GamingWalletLedger, playerCoins, "COINS")
	if !ok || bal.Cmp(big.NewInt(100)) < 0 {
		return nil, ErrSkip
	}

	costs := []int64{100, 250, 500}
	cost := costs[RandIntN(r, len(costs))]
	if bal.Cmp(big.NewInt(cost)) < 0 {
		cost = 100
		if bal.Cmp(big.NewInt(cost)) < 0 {
			return nil, ErrSkip
		}
	}

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(GamingWalletLedger, "buy_item", "1.0.0", map[string]string{
			"player_coins": playerCoins,
			"amount":       fmt.Sprintf("COINS %d", cost),
		}, map[string]string{"item": fmt.Sprintf("item-%d", RandIntN(r, 100))}),
	)
}

func gamingP2PTrade(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	fromID := 1 + RandIntN(r, GamingWalletNumPlayers)
	toID := 1 + RandIntN(r, GamingWalletNumPlayers)
	if fromID == toID {
		toID = 1 + fromID%GamingWalletNumPlayers
	}

	fromCoins := fmt.Sprintf("player:%d:coins", fromID)
	toCoins := fmt.Sprintf("player:%d:coins", toID)

	bal, ok := GetAccountBalance(ctx, client, GamingWalletLedger, fromCoins, "COINS")
	if !ok || bal.Cmp(big.NewInt(10)) < 0 {
		return nil, ErrSkip
	}

	amount := int64(10) + RandInt64N(r, bal.Int64()-9)

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(GamingWalletLedger, "p2p_transfer", "1.0.0", map[string]string{
			"from_player": fromCoins,
			"to_player":   toCoins,
			"amount":      fmt.Sprintf("COINS %d", amount),
		}, map[string]string{"type": "p2p-trade"}),
	)
}

func gamingPromotion(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	playerID := 1 + RandIntN(r, GamingWalletNumPlayers)
	promoCoins := int64(100 + RandIntN(r, 1000))
	playerAddr := fmt.Sprintf("player:%d:coins", playerID)

	return ApplyActions(ctx, client,
		actions.CreateForceTransactionAction(GamingWalletLedger, []*commonpb.Posting{
			actions.NewPosting("world", playerAddr, big.NewInt(promoCoins), "COINS"),
		}, map[string]string{"type": "promotion", "reason": "bonus"}),
	)
}

func gamingClawback(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	playerID := 1 + RandIntN(r, GamingWalletNumPlayers)
	playerCoins := fmt.Sprintf("player:%d:coins", playerID)

	bal, ok := GetAccountBalance(ctx, client, GamingWalletLedger, playerCoins, "COINS")
	if !ok || bal.Cmp(big.NewInt(50)) < 0 {
		return nil, ErrSkip
	}

	amount := int64(50) + RandInt64N(r, bal.Int64()-49)

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(GamingWalletLedger, "clawback", "1.0.0", map[string]string{
			"player_coins": playerCoins,
			"amount":       fmt.Sprintf("COINS %d", amount),
		}, map[string]string{"type": "clawback"}),
	)
}

// GamingWalletSetupActions returns the Apply requests that create the ledger,
// account types, and numscript library for the gaming wallet scenario.
func GamingWalletSetupActions() []*servicepb.Request {
	return []*servicepb.Request{
		actions.CreateLedgerAction(GamingWalletLedger, nil),
		actions.AddAccountTypeAction(GamingWalletLedger, "player-usd", "player:{id}:usd"),
		actions.AddAccountTypeAction(GamingWalletLedger, "player-coins", "player:{id}:coins"),
		actions.AddAccountTypeAction(GamingWalletLedger, "platform", "platform:{type}"),
		actions.AddAccountTypeAction(GamingWalletLedger, "shop", "shop:{type}"),
		actions.AddAccountTypeAction(GamingWalletLedger, "escrow", "escrow:{type}"),
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
			action.GetApply().GetAction().GetCreateTransaction().Reference = fmt.Sprintf("topup-initial-%d", i)
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
	numTrades := r.Iterations(10)
	{
		var reqs []*servicepb.Request
		for t := range numTrades {
			trade := [3]int{1 + t%numPlayers, 1 + (t+1)%numPlayers, 30 + t*10}
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
