package scenario

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
)

func init() { Register("multi-currency", RunMultiCurrency) }

// MultiCurrencyLedger is the ledger name used by the multi-currency scenario.
const MultiCurrencyLedger = "treasury"

// MultiCurrencyCurrency describes a currency and its treasury account.
type MultiCurrencyCurrency struct {
	Asset    string
	Treasury string
}

// MultiCurrencyCurrencies returns the available currencies.
func MultiCurrencyCurrencies() []MultiCurrencyCurrency {
	return []MultiCurrencyCurrency{
		{"USD/2", "treasury:usd"},
		{"EUR/2", "treasury:eur"},
		{"GBP/2", "treasury:gbp"},
	}
}

// MultiCurrencyVendors returns the available vendors.
func MultiCurrencyVendors() []string {
	return []string{
		"vendor:acme", "vendor:globex", "vendor:initech",
		"vendor:umbrella", "vendor:stark", "vendor:brit-co",
	}
}

// MultiCurrencyBlocks returns the atomic blocks for the multi-currency scenario.
func MultiCurrencyBlocks() *BlockGroup {
	return &BlockGroup{
		Setup: MultiCurrencySetupActions,
		Blocks: []*Block{
			{Name: "multicurrency/fund", Run: multiCurrencyFund},
			{Name: "multicurrency/fx", Run: multiCurrencyFX},
			{Name: "multicurrency/vendor_pay", Run: multiCurrencyVendorPay},
		},
	}
}

func multiCurrencyFund(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	currencies := MultiCurrencyCurrencies()
	cur := currencies[RandIntN(r, len(currencies))]
	amount := int64(100_000 + RandIntN(r, 900_000))

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(MultiCurrencyLedger, "fund_account", "1.0.0", map[string]string{
			"account": cur.Treasury,
			"amount":  fmt.Sprintf("%s %d", cur.Asset, amount),
		}, nil),
	)
}

func multiCurrencyFX(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	currencies := MultiCurrencyCurrencies()
	srcIdx := RandIntN(r, len(currencies))
	dstIdx := (srcIdx + 1 + RandIntN(r, len(currencies)-1)) % len(currencies)
	src := currencies[srcIdx]
	dst := currencies[dstIdx]

	bal, ok := GetAccountBalance(ctx, client, MultiCurrencyLedger, src.Treasury, src.Asset)
	if !ok || bal.Cmp(big.NewInt(5000)) < 0 {
		return nil, ErrSkip
	}

	srcAmount := min(int64(5000)+RandInt64N(r, bal.Int64()-4999), 50_000)
	dstAmount := srcAmount * 92 / 100

	// Leg 1: source -> fx:clearing.
	_, err := ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(MultiCurrencyLedger, "fx_convert", "1.0.0", map[string]string{
			"source_account":   src.Treasury,
			"clearing_account": "fx:clearing",
			"amount":           fmt.Sprintf("%s %d", src.Asset, srcAmount),
		}, nil),
	)
	if err != nil {
		return nil, err
	}

	// Leg 2: fx:clearing -> target (force, different currency).
	return ApplyActions(ctx, client,
		actions.CreateForceTransactionAction(MultiCurrencyLedger, []*commonpb.Posting{
			actions.NewPosting("fx:clearing", dst.Treasury, big.NewInt(dstAmount), dst.Asset),
		}, nil),
	)
}

func multiCurrencyVendorPay(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	currencies := MultiCurrencyCurrencies()
	vendors := MultiCurrencyVendors()
	cur := currencies[RandIntN(r, len(currencies))]
	vendor := vendors[RandIntN(r, len(vendors))]

	bal, ok := GetAccountBalance(ctx, client, MultiCurrencyLedger, cur.Treasury, cur.Asset)
	if !ok || bal.Cmp(big.NewInt(400)) < 0 {
		return nil, ErrSkip
	}

	amount := int64(400) + RandInt64N(r, min(bal.Int64()-399, 10_000))

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(MultiCurrencyLedger, "vendor_payment", "1.0.0", map[string]string{
			"treasury": cur.Treasury,
			"vendor":   vendor,
			"amount":   fmt.Sprintf("%s %d", cur.Asset, amount),
		}, nil),
	)
}

// MultiCurrencySetupActions returns the Apply requests that create the ledger,
// account types, and numscript library for the multi-currency scenario.
func MultiCurrencySetupActions() []*servicepb.Request {
	return []*servicepb.Request{
		actions.CreateLedgerAction(MultiCurrencyLedger, nil),
		actions.AddAccountTypeAction(MultiCurrencyLedger, "treasury", "treasury:{currency}"),
		actions.AddEphemeralAccountTypeAction(MultiCurrencyLedger, "fx-clearing", "fx:clearing"),
		actions.AddAccountTypeAction(MultiCurrencyLedger, "vendor", "vendor:{name}"),
		actions.SaveNumscriptWithVersionAction(MultiCurrencyLedger, "fund_account", `vars {
  account $account
  monetary $amount
}
send $amount (
  source = @world
  destination = $account
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(MultiCurrencyLedger, "fx_convert", `vars {
  account $source_account
  account $clearing_account
  monetary $amount
}
send $amount (
  source = $source_account
  destination = $clearing_account
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(MultiCurrencyLedger, "vendor_payment", `vars {
  account $treasury
  account $vendor
  monetary $amount
}
send $amount (
  source = $treasury
  destination = $vendor
)`, "1.0.0"),
		actions.CreateBuiltinTxIndexAction(MultiCurrencyLedger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP),
		actions.CreateBuiltinTxIndexAction(MultiCurrencyLedger, commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT),
		actions.CreatePreparedQueryAction("accounts-by-prefix", MultiCurrencyLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamAddressPrefixFilter("prefix"),
		),
		actions.CreatePreparedQueryAction("account-exact", MultiCurrencyLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamAddressExactFilter("addr"),
		),
		actions.CreatePreparedQueryAction("volumes-by-prefix", MultiCurrencyLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamAddressPrefixFilter("prefix"),
		),
	}
}

// RunMultiCurrency provisions a corporate treasury scenario with multiple currencies
// and FX operations through a clearing account, plus vendor payments.
func RunMultiCurrency(r *Runner) error {
	ledger := MultiCurrencyLedger

	type fxOp struct {
		sourceAccount string
		sourceAsset   string
		sourceAmount  int64
		targetAccount string
		targetAsset   string
		targetAmount  int64
	}

	fxOps := []fxOp{
		{"treasury:usd", "USD/2", 10_000, "treasury:eur", "EUR/2", 9_200},
		{"treasury:usd", "USD/2", 15_000, "treasury:eur", "EUR/2", 13_800},
		{"treasury:usd", "USD/2", 8_000, "treasury:eur", "EUR/2", 7_360},
		{"treasury:usd", "USD/2", 20_000, "treasury:eur", "EUR/2", 18_500},
		{"treasury:eur", "EUR/2", 5_000, "treasury:gbp", "GBP/2", 4_300},
		{"treasury:eur", "EUR/2", 8_000, "treasury:gbp", "GBP/2", 6_880},
		{"treasury:eur", "EUR/2", 3_000, "treasury:gbp", "GBP/2", 2_580},
		{"treasury:eur", "EUR/2", 12_000, "treasury:gbp", "GBP/2", 10_320},
		{"treasury:gbp", "GBP/2", 6_000, "treasury:usd", "USD/2", 7_620},
		{"treasury:gbp", "GBP/2", 4_000, "treasury:usd", "USD/2", 5_080},
		{"treasury:gbp", "GBP/2", 2_000, "treasury:usd", "USD/2", 2_540},
		{"treasury:gbp", "GBP/2", 3_000, "treasury:usd", "USD/2", 3_810},
		{"treasury:usd", "USD/2", 7_000, "treasury:gbp", "GBP/2", 5_530},
		{"treasury:usd", "USD/2", 9_000, "treasury:gbp", "GBP/2", 7_110},
		{"treasury:usd", "USD/2", 5_000, "treasury:gbp", "GBP/2", 3_950},
		{"treasury:usd", "USD/2", 11_000, "treasury:gbp", "GBP/2", 8_690},
		{"treasury:eur", "EUR/2", 6_000, "treasury:usd", "USD/2", 6_540},
		{"treasury:eur", "EUR/2", 10_000, "treasury:usd", "USD/2", 10_900},
		{"treasury:eur", "EUR/2", 4_000, "treasury:usd", "USD/2", 4_360},
		{"treasury:eur", "EUR/2", 7_000, "treasury:usd", "USD/2", 7_630},
	}

	type vendorPayment struct {
		treasury string
		vendor   string
		asset    string
		amount   int64
	}

	numEURPayments := r.Iterations(30)
	numGBPPayments := r.Iterations(20)

	eurVendors := []string{"vendor:acme", "vendor:globex", "vendor:initech", "vendor:umbrella", "vendor:stark"}
	var eurPayments []vendorPayment
	for i := range numEURPayments {
		eurPayments = append(eurPayments, vendorPayment{
			treasury: "treasury:eur",
			vendor:   eurVendors[i%len(eurVendors)],
			asset:    "EUR/2",
			amount:   int64(500 + i*100),
		})
	}

	gbpVendors := []string{"vendor:brit-co", "vendor:london-ltd", "vendor:windsor", "vendor:thames"}
	var gbpPayments []vendorPayment
	for i := range numGBPPayments {
		gbpPayments = append(gbpPayments, vendorPayment{
			treasury: "treasury:gbp",
			vendor:   gbpVendors[i%len(gbpVendors)],
			asset:    "GBP/2",
			amount:   int64(400 + i*80),
		})
	}

	// --- Setup ---
	if _, err := r.Step("Setup", MultiCurrencySetupActions()...); err != nil {
		return err
	}

	// Fund treasury accounts
	if _, err := r.Step("FundTreasury",
		actions.CreateScriptRefTransactionAction(ledger, "fund_account", "1.0.0", map[string]string{
			"account": "treasury:usd",
			"amount":  "USD/2 1000000",
		}, nil),
		actions.CreateScriptRefTransactionAction(ledger, "fund_account", "1.0.0", map[string]string{
			"account": "treasury:eur",
			"amount":  "EUR/2 500000",
		}, nil),
		actions.CreateScriptRefTransactionAction(ledger, "fund_account", "1.0.0", map[string]string{
			"account": "treasury:gbp",
			"amount":  "GBP/2 300000",
		}, nil),
	); err != nil {
		return err
	}

	// --- FX Operations (batched: leg1+leg2 pairs) ---
	{
		numFXOps := min(r.Iterations(len(fxOps)), len(fxOps))
		reqs := make([]*servicepb.Request, 0, numFXOps*2)
		for _, fx := range fxOps[:numFXOps] {
			reqs = append(reqs,
				actions.CreateScriptRefTransactionAction(ledger, "fx_convert", "1.0.0", map[string]string{
					"source_account":   fx.sourceAccount,
					"clearing_account": "fx:clearing",
					"amount":           fmt.Sprintf("%s %d", fx.sourceAsset, fx.sourceAmount),
				}, nil),
				actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
					actions.NewPosting("fx:clearing", fx.targetAccount, big.NewInt(fx.targetAmount), fx.targetAsset),
				}, nil),
			)
		}
		if _, err := r.Step("FXOperations", reqs...); err != nil {
			return err
		}
	}

	// --- Vendor Payments (batched) ---
	{
		reqs := make([]*servicepb.Request, 0, len(eurPayments)+len(gbpPayments))
		for _, vp := range eurPayments {
			reqs = append(reqs, actions.CreateScriptRefTransactionAction(ledger, "vendor_payment", "1.0.0", map[string]string{
				"treasury": vp.treasury,
				"vendor":   vp.vendor,
				"amount":   fmt.Sprintf("%s %d", vp.asset, vp.amount),
			}, nil))
		}
		for _, vp := range gbpPayments {
			reqs = append(reqs, actions.CreateScriptRefTransactionAction(ledger, "vendor_payment", "1.0.0", map[string]string{
				"treasury": vp.treasury,
				"vendor":   vp.vendor,
				"amount":   fmt.Sprintf("%s %d", vp.asset, vp.amount),
			}, nil))
		}
		if _, err := r.Step("VendorPayments", reqs...); err != nil {
			return err
		}
	}

	return nil
}
