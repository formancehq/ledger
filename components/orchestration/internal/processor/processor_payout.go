package processor

import (
	"errors"
	"fmt"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/formancehq/orchestration/internal/activities"
	"github.com/formancehq/orchestration/internal/crawler"
	"github.com/formancehq/orchestration/internal/spec"
	"go.temporal.io/sdk/workflow"
)

const stripeConnectIDMetadata = "stripeConnectID"

func payoutFromWallet(ctx workflow.Context, walletID, balance, asset string, amount uint64) error {

	wallet, err := activities.GetWallet(singleTryContext(ctx), walletID)
	if err != nil {
		return err
	}
	if _, ok := wallet.Metadata[stripeConnectIDMetadata]; !ok {
		return errors.New("stripe connect ID not found")
	}

	stripeConnectID, ok := wallet.Metadata[stripeConnectIDMetadata].(string)
	if !ok {
		return errors.New("invalid format for stripe connect ID")
	}

	hold, err := activities.DebitWallet(singleTryContext(ctx), walletID, sdk.DebitWalletRequest{
		Amount: sdk.Monetary{
			Asset:  asset,
			Amount: int64(amount),
		},
		Pending:  sdk.PtrBool(true),
		Balances: []string{balance},
	})
	if err != nil {
		return err
	}

	if err := activities.StripeTransfer(singleTryContext(ctx), sdk.StripeTransferRequest{
		Amount:      sdk.PtrInt64(int64(amount)),
		Asset:       &asset,
		Destination: &stripeConnectID,
	}); err != nil {
		if err := activities.VoidHold(infiniteRetryContext(ctx), hold.Id); err != nil {
			return err
		}
		return err
	}

	if err := activities.ConfirmHold(infiniteRetryContext(ctx), hold.Id); err != nil {
		return err
	}

	return nil
}

func Payout(ctx workflow.Context, input Input) (any, error) {
	crawler := crawler.New(input.Specification.ObjectSchema, input.Parameters,
		crawler.NewContext().WithVariables(input.Variables))
	sourceCrawler := crawler.GetProperty("source").AsDiscriminated()
	amountCrawler := crawler.GetProperty("amount")

	switch sourceCrawler.GetType() {
	case "wallet":
		return nil, payoutFromWallet(ctx,
			sourceCrawler.GetProperty("wallet").AsString(),
			sourceCrawler.GetProperty("balance").AsString(),
			amountCrawler.GetProperty("asset").AsString(),
			amountCrawler.GetProperty("amount").AsUInt64(),
		)
	default:
		return nil, fmt.Errorf("source type %s", sourceCrawler.GetType())
	}
}

func init() {
	RegisterProcessor(spec.PayoutLabel, Func(Payout))
}
