package activities

import (
	"context"

	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"go.temporal.io/sdk/workflow"
)

func (a Activities) ConfirmHold(ctx context.Context, id string) error {
	_, err := a.client.WalletsApi.
		ConfirmHold(ctx, id).
		Execute()
	if err != nil {
		return formance.ExtractOpenAPIErrorMessage(err)
	}
	return nil
}

var ConfirmHoldActivity = Activities{}.ConfirmHold

func ConfirmHold(ctx workflow.Context, id string) error {
	if err := workflow.ExecuteActivity(ctx, ConfirmHoldActivity, id).Get(ctx, nil); err != nil {
		return errors.Unwrap(err)
	}
	return nil
}
