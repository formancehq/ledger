package activities

import (
	"context"
	"net/http"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"go.temporal.io/sdk/workflow"
)

func (a Activities) CreditWallet(ctx context.Context, id string, request sdk.CreditWalletRequest) error {
	httpResponse, err := a.client.WalletsApi.
		CreditWallet(ctx, id).
		CreditWalletRequest(request).
		Execute()
	if err != nil {
		switch httpResponse.StatusCode {
		case http.StatusNotFound:
			return errors.New("wallet not found")
		default:
			return sdk.ExtractOpenAPIErrorMessage(err)
		}
	}
	return nil
}

var CreditWalletActivity = Activities{}.CreditWallet

func CreditWallet(ctx workflow.Context, id string, request sdk.CreditWalletRequest) error {
	if err := workflow.ExecuteActivity(ctx, CreditWalletActivity, id, request).Get(ctx, nil); err != nil {
		return errors.Unwrap(err)
	}
	return nil
}
