package activities

import (
	"context"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"go.temporal.io/sdk/workflow"
)

func (a Activities) DebitWallet(ctx context.Context, id string, request sdk.DebitWalletRequest) (any, error) {
	ret, _, err := a.client.WalletsApi.
		DebitWallet(ctx, id).
		DebitWalletRequest(request).
		Execute()
	if err != nil {
		return nil, sdk.ExtractOpenAPIErrorMessage(err)
	}
	return ret.Data, nil
}

var DebitWalletActivity = Activities{}.DebitWallet

func DebitWallet(ctx workflow.Context, id string, request sdk.DebitWalletRequest) (*sdk.Hold, error) {
	ret := &sdk.DebitWalletResponse{}
	if err := workflow.ExecuteActivity(ctx, DebitWalletActivity, id, request).Get(ctx, ret); err != nil {
		return nil, errors.Unwrap(err)
	}
	return &ret.Data, nil
}
