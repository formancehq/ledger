package scenario

import (
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/scenario/actions"
)

func init() { Register("multi-currency", RunMultiCurrency) }

// MultiCurrencyLedger is the ledger name used by the multi-currency scenario.
const MultiCurrencyLedger = "treasury"

// MultiCurrencySetupActions returns the Apply requests that create the ledger,
// account types, and numscript library for the multi-currency scenario.
func MultiCurrencySetupActions() []*servicepb.Request {
	return []*servicepb.Request{
		actions.CreateLedgerAction(MultiCurrencyLedger, nil),
		actions.AddAccountTypeAction(MultiCurrencyLedger, "treasury", "treasury:{currency}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(MultiCurrencyLedger, "fx-clearing", "fx:clearing", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(MultiCurrencyLedger, "vendor", "vendor:{name}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
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

	eurVendors := []string{"vendor:acme", "vendor:globex", "vendor:initech", "vendor:umbrella", "vendor:stark"}
	var eurPayments []vendorPayment
	for i := range 30 {
		eurPayments = append(eurPayments, vendorPayment{
			treasury: "treasury:eur",
			vendor:   eurVendors[i%len(eurVendors)],
			asset:    "EUR/2",
			amount:   int64(500 + i*100),
		})
	}

	gbpVendors := []string{"vendor:brit-co", "vendor:london-ltd", "vendor:windsor", "vendor:thames"}
	var gbpPayments []vendorPayment
	for i := range 20 {
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

	// --- FX Operations (20 ops = 40 Apply calls) ---
	for i, fx := range fxOps {
		// Leg 1: source -> fx:clearing (balance-checked)
		if _, err := r.Step(fmt.Sprintf("FX/%d/Leg1", i),
			actions.CreateScriptRefTransactionAction(ledger, "fx_convert", "1.0.0", map[string]string{
				"source_account":   fx.sourceAccount,
				"clearing_account": "fx:clearing",
				"amount":           fmt.Sprintf("%s %d", fx.sourceAsset, fx.sourceAmount),
			}, nil),
		); err != nil {
			return err
		}

		// Leg 2: fx:clearing -> target (force, different currency)
		if _, err := r.Step(fmt.Sprintf("FX/%d/Leg2", i),
			actions.CreateForceTransactionAction(ledger, []*commonpb.Posting{
				actions.NewPosting("fx:clearing", fx.targetAccount, big.NewInt(fx.targetAmount), fx.targetAsset),
			}, nil),
		); err != nil {
			return err
		}
	}

	// --- Vendor Payments (30 EUR + 20 GBP) ---
	for i, vp := range eurPayments {
		if _, err := r.Step(fmt.Sprintf("VendorPayment/EUR/%d", i),
			actions.CreateScriptRefTransactionAction(ledger, "vendor_payment", "1.0.0", map[string]string{
				"treasury": vp.treasury,
				"vendor":   vp.vendor,
				"amount":   fmt.Sprintf("%s %d", vp.asset, vp.amount),
			}, nil),
		); err != nil {
			return err
		}
	}
	for i, vp := range gbpPayments {
		if _, err := r.Step(fmt.Sprintf("VendorPayment/GBP/%d", i),
			actions.CreateScriptRefTransactionAction(ledger, "vendor_payment", "1.0.0", map[string]string{
				"treasury": vp.treasury,
				"vendor":   vp.vendor,
				"amount":   fmt.Sprintf("%s %d", vp.asset, vp.amount),
			}, nil),
		); err != nil {
			return err
		}
	}

	return nil
}
