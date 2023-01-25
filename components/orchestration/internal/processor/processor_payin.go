package processor

import (
	"fmt"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/formancehq/orchestration/internal/activities"
	"github.com/formancehq/orchestration/internal/crawler"
	"github.com/formancehq/orchestration/internal/spec"
	"github.com/pkg/errors"
	"go.temporal.io/sdk/workflow"
)

func payinToWallet(ctx workflow.Context, walletID, balance, paymentID string) error {

	// TODO: Retry for a delay ?
	payment, err := activities.GetPayment(singleTryContext(ctx), paymentID)
	if err != nil {
		return errors.Wrap(err, "retrieving payment")
	}

	if payment.Status != sdk.TERMINATED {
		return errors.Wrap(err, "payment not terminated")
	}

	wallet, err := activities.GetWallet(singleTryContext(ctx), walletID)
	if err != nil {
		return errors.Wrap(err, "retrieving wallet")
	}

	ledgerAccount := fmt.Sprintf("orchestration:payins:%s", paymentID)
	_, err = activities.CreateTransaction(singleTryContext(ctx), wallet.Ledger, sdk.PostTransaction{
		Postings: []sdk.Posting{{
			Amount:      payment.InitialAmount,
			Asset:       payment.Asset,
			Destination: ledgerAccount,
			Source:      "world",
		}},
	})
	if err != nil {
		return errors.Wrap(err, "creating payin transaction")
	}

	err = activities.CreditWallet(infiniteRetryContext(ctx), walletID, sdk.CreditWalletRequest{
		Amount: sdk.Monetary{
			Asset:  payment.Asset,
			Amount: payment.InitialAmount,
		},
		Sources: []sdk.Subject{{
			LedgerAccountSubject: sdk.NewLedgerAccountSubject("LEDGER", ledgerAccount),
		}},
		Balance: &balance,
	})
	if err != nil {
		return errors.Wrap(err, "crediting wallet")
	}

	return nil
}

func Payin(ctx workflow.Context, input Input) (any, error) {
	crawler := crawler.New(input.Specification.ObjectSchema, input.Parameters,
		crawler.NewContext().WithVariables(input.Variables))
	destinationCrawler := crawler.GetProperty("destination").AsDiscriminated()

	switch destinationCrawler.GetType() {
	case "wallet":
		return nil, payinToWallet(ctx,
			destinationCrawler.GetProperty("wallet").AsString(),
			destinationCrawler.GetProperty("balance").AsString(),
			crawler.GetProperty("source").AsString(),
		)
	default:
		return nil, fmt.Errorf("destination type %s", destinationCrawler.GetType())
	}
}

func init() {
	RegisterProcessor(spec.PayinLabel, Func(Payin))
}
