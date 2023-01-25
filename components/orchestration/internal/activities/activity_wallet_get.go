package activities

import (
	"context"
	"errors"
	"net/http"

	sdk "github.com/formancehq/formance-sdk-go"
	"go.temporal.io/sdk/workflow"
)

func (a Activities) GetWallet(ctx context.Context, id string) (*sdk.GetWalletResponse, error) {
	ret, httpResponse, err := a.client.WalletsApi.
		GetWallet(ctx, id).
		Execute()
	if err != nil {
		switch httpResponse.StatusCode {
		case http.StatusNotFound:
			return nil, errors.New("wallet not found")
		default:
			return nil, sdk.ExtractOpenAPIErrorMessage(err)
		}
	}
	return ret, nil
}

var GetWalletActivity = Activities{}.GetWallet

func GetWallet(ctx workflow.Context, id string) (*sdk.WalletWithBalances, error) {
	ret := &sdk.GetWalletResponse{}
	if err := workflow.ExecuteActivity(ctx, GetWalletActivity, id).Get(ctx, ret); err != nil {
		return nil, errors.Unwrap(err)
	}
	return &ret.Data, nil
}
