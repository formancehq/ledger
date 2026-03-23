package scenario

import (
	"context"
	"fmt"
	"math/big"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
)

func init() { Register("subscription", RunSubscription) }

const (
	// SubscriptionLedger is the ledger name used by the subscription scenario.
	SubscriptionLedger         = "billing"
	SubscriptionNumSubscribers = 50
)

// SubscriptionTier describes a subscription tier with its pricing.
type SubscriptionTier struct {
	Name   string
	Amount int64
	Fund   int64
}

// SubscriptionTiers returns the available subscription tiers.
func SubscriptionTiers() []SubscriptionTier {
	return []SubscriptionTier{
		{"basic", 1000, 5000},
		{"pro", 2500, 10000},
		{"enterprise", 5000, 20000},
	}
}

// SubscriptionBlocks returns the atomic blocks for the subscription scenario.
func SubscriptionBlocks() *BlockGroup {
	return &BlockGroup{
		Setup: SubscriptionSetupActions,
		Blocks: []*Block{
			{Name: "subscription/fund", Run: subscriptionFund},
			{Name: "subscription/charge", Run: subscriptionCharge},
			{Name: "subscription/recognize", Run: subscriptionRecognize},
			{Name: "subscription/credit", Run: subscriptionCredit},
		},
	}
}

func subscriptionFund(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	subID := 1 + RandIntN(r, SubscriptionNumSubscribers)
	tiers := SubscriptionTiers()
	tier := tiers[RandIntN(r, len(tiers))]
	address := fmt.Sprintf("subscriber:%d", subID)

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(SubscriptionLedger, "fund_wallet", "1.0.0", map[string]string{
			"subscriber": address,
			"amount":     fmt.Sprintf("USD/2 %d", tier.Fund),
		}, nil),
	)
}

func subscriptionCharge(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	subID := 1 + RandIntN(r, SubscriptionNumSubscribers)
	tiers := SubscriptionTiers()
	tier := tiers[(subID-1)%len(tiers)]
	address := fmt.Sprintf("subscriber:%d", subID)

	bal, ok := GetAccountBalance(ctx, client, SubscriptionLedger, address, "USD/2")
	if !ok || bal.Cmp(big.NewInt(tier.Amount)) < 0 {
		return nil, ErrSkip
	}

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(SubscriptionLedger, "charge_subscription", "1.0.0", map[string]string{
			"subscriber": address,
			"amount":     fmt.Sprintf("USD/2 %d", tier.Amount),
		}, nil),
	)
}

func subscriptionRecognize(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	bal, ok := GetAccountBalance(ctx, client, SubscriptionLedger, "revenue:deferred", "USD/2")
	if !ok || bal.Sign() <= 0 {
		return nil, ErrSkip
	}

	portion := int64(25 + RandIntN(r, 76))
	amount := new(big.Int).Mul(bal, big.NewInt(portion))
	amount.Div(amount, big.NewInt(100))
	if amount.Sign() <= 0 {
		return nil, ErrSkip
	}

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(SubscriptionLedger, "recognize_revenue", "1.0.0", map[string]string{
			"amount": fmt.Sprintf("USD/2 %d", amount.Int64()),
		}, nil),
	)
}

func subscriptionCredit(ctx context.Context, client servicepb.BucketServiceClient, r RandFunc) (*servicepb.ApplyResponse, error) {
	subID := 1 + RandIntN(r, SubscriptionNumSubscribers)
	amount := int64(100 + RandIntN(r, 500))
	address := fmt.Sprintf("subscriber:%d", subID)

	return ApplyActions(ctx, client,
		actions.CreateScriptRefTransactionAction(SubscriptionLedger, "issue_credit", "1.0.0", map[string]string{
			"subscriber": address,
			"amount":     fmt.Sprintf("USD/2 %d", amount),
		}, nil),
	)
}

// SubscriptionSetupActions returns the Apply requests that create the ledger,
// schema, account types, and numscript library for the subscription scenario.
func SubscriptionSetupActions() []*servicepb.Request {
	return []*servicepb.Request{
		actions.CreateLedgerWithSchemaAction(SubscriptionLedger, nil, []*commonpb.SetMetadataFieldTypeCommand{
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "subscriber_plan",
				Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
			},
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "billing_cycle",
				Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
			},
			{
				TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "retention_score",
				Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
			},
		}),
		actions.AddAccountTypeAction(SubscriptionLedger, "subscriber", "subscriber:{id}", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(SubscriptionLedger, "revenue-deferred", "revenue:deferred", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(SubscriptionLedger, "revenue-recognized", "revenue:recognized", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.AddAccountTypeAction(SubscriptionLedger, "revenue-adjustment", "revenue:adjustment", commonpb.ChartEnforcementMode_CHART_ENFORCEMENT_STRICT),
		actions.SaveNumscriptWithVersionAction(SubscriptionLedger, "fund_wallet", `vars {
  account $subscriber
  monetary $amount
}
send $amount (
  source = @world
  destination = $subscriber
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(SubscriptionLedger, "charge_subscription", `vars {
  account $subscriber
  monetary $amount
}
send $amount (
  source = $subscriber
  destination = @revenue:deferred
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(SubscriptionLedger, "issue_credit", `vars {
  account $subscriber
  monetary $amount
}
send $amount (
  source = @world
  destination = $subscriber
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(SubscriptionLedger, "recognize_revenue", `vars {
  monetary $amount
}
send $amount (
  source = @revenue:deferred allowing unbounded overdraft
  destination = @revenue:recognized
)`, "1.0.0"),
		actions.SaveNumscriptWithVersionAction(SubscriptionLedger, "adjust_revenue", `vars {
  monetary $amount
}
send $amount (
  source = @revenue:deferred allowing unbounded overdraft
  destination = @revenue:adjustment
)`, "1.0.0"),
		actions.CreateAccountMetadataIndexAction(SubscriptionLedger, "subscriber_plan"),
		actions.CreateAccountMetadataIndexAction(SubscriptionLedger, "retention_score"),
		actions.CreatePreparedQueryAction("accounts-by-prefix", SubscriptionLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamAddressPrefixFilter("prefix"),
		),
		actions.CreatePreparedQueryAction("by-plan", SubscriptionLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamStringMetadataFilter("subscriber_plan", "plan_value"),
		),
		actions.CreatePreparedQueryAction("high-retention", SubscriptionLedger,
			commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS,
			actions.ParamInt64RangeMetadataFilter("retention_score", "min_score", "max_score"),
		),
	}
}

// RunSubscription provisions a SaaS billing scenario with 50 subscribers
// over 3 monthly cycles, with credits, revenue adjustments, and recognition.
func RunSubscription(r *Runner) error {
	const (
		numSubscribers = 50
		numUnderFunded = 5
	)

	numCycles := r.Iterations(3)

	type subscriber struct {
		id       int
		tier     string
		amount   int64
		fundedAt int64
	}

	tiers := []struct {
		name   string
		amount int64
		fund   int64
	}{
		{"basic", 1000, 5000},
		{"pro", 2500, 10000},
		{"enterprise", 5000, 20000},
	}

	subscribers := make([]subscriber, 0, numSubscribers)
	for i := 1; i <= numSubscribers; i++ {
		tier := tiers[(i-1)%len(tiers)]
		fund := tier.fund
		if i <= numUnderFunded {
			fund = tier.amount / (int64(numCycles) + 1)
		}
		subscribers = append(subscribers, subscriber{
			id:       i,
			tier:     tier.name,
			amount:   tier.amount,
			fundedAt: fund,
		})
	}

	var (
		totalDeferred    = new(big.Int)
		adjustmentAmount = big.NewInt(500)
	)

	// --- Setup ---
	if _, err := r.Step("Setup", SubscriptionSetupActions()...); err != nil {
		return err
	}

	// --- Billing Cycles ---
	for cycle := 1; cycle <= numCycles; cycle++ {
		// Fund wallets
		fundReqs := make([]*servicepb.Request, 0, numSubscribers)
		for _, sub := range subscribers {
			fundReqs = append(fundReqs, actions.CreateScriptRefTransactionAction(SubscriptionLedger, "fund_wallet", "1.0.0", map[string]string{
				"subscriber": fmt.Sprintf("subscriber:%d", sub.id),
				"amount":     fmt.Sprintf("USD/2 %d", sub.fundedAt),
			}, nil))
		}
		if _, err := r.Step(fmt.Sprintf("Cycle%d/FundWallets", cycle), fundReqs...); err != nil {
			return err
		}

		// Billing: charge only successfully funded subscribers
		var billingReqs []*servicepb.Request
		for _, sub := range subscribers {
			if sub.fundedAt < sub.amount {
				continue // skip under-funded
			}
			billingReqs = append(billingReqs, actions.CreateScriptRefTransactionAction(SubscriptionLedger, "charge_subscription", "1.0.0", map[string]string{
				"subscriber": fmt.Sprintf("subscriber:%d", sub.id),
				"amount":     fmt.Sprintf("USD/2 %d", sub.amount),
			}, nil))
			totalDeferred.Add(totalDeferred, big.NewInt(sub.amount))
		}
		if _, err := r.Step(fmt.Sprintf("Cycle%d/Billing", cycle), billingReqs...); err != nil {
			return err
		}

		// Credits to 2 subscribers
		if _, err := r.Step(fmt.Sprintf("Cycle%d/Credits", cycle),
			actions.CreateScriptRefTransactionAction(SubscriptionLedger, "issue_credit", "1.0.0", map[string]string{
				"subscriber": fmt.Sprintf("subscriber:%d", 6+cycle),
				"amount":     "USD/2 200",
			}, nil),
			actions.CreateScriptRefTransactionAction(SubscriptionLedger, "issue_credit", "1.0.0", map[string]string{
				"subscriber": fmt.Sprintf("subscriber:%d", 10+cycle),
				"amount":     "USD/2 300",
			}, nil),
		); err != nil {
			return err
		}

		// Typed metadata on first cycle
		if cycle == 1 {
			if _, err := r.Step("Cycle1/TypedMetadata",
				actions.SaveTypedAccountMetadataAction(SubscriptionLedger, "subscriber:6", &commonpb.MetadataSet{
					Metadata: []*commonpb.Metadata{
						{Key: "subscriber_plan", Value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "pro"}}},
						{Key: "billing_cycle", Value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_IntValue{IntValue: 1}}},
					},
				}),
				actions.SaveTypedAccountMetadataAction(SubscriptionLedger, "subscriber:7", &commonpb.MetadataSet{
					Metadata: []*commonpb.Metadata{
						{Key: "subscriber_plan", Value: &commonpb.MetadataValue{Type: &commonpb.MetadataValue_StringValue{StringValue: "enterprise"}}},
					},
				}),
			); err != nil {
				return err
			}
		}

		// Revenue adjustment
		if _, err := r.Step(fmt.Sprintf("Cycle%d/Adjustment", cycle),
			actions.CreateScriptRefTransactionAction(SubscriptionLedger, "adjust_revenue", "1.0.0", map[string]string{
				"amount": fmt.Sprintf("USD/2 %d", adjustmentAmount.Int64()),
			}, nil),
		); err != nil {
			return err
		}
		totalDeferred.Sub(totalDeferred, adjustmentAmount)

		// Revenue recognition
		recognizeAmt := new(big.Int).Set(totalDeferred)
		if _, err := r.Step(fmt.Sprintf("Cycle%d/RevenueRecognition", cycle),
			actions.CreateScriptRefTransactionAction(SubscriptionLedger, "recognize_revenue", "1.0.0", map[string]string{
				"amount": fmt.Sprintf("USD/2 %d", recognizeAmt.Int64()),
			}, nil),
		); err != nil {
			return err
		}
		totalDeferred.Sub(totalDeferred, recognizeAmt)
	}

	return nil
}
