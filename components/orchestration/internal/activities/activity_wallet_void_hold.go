package activities

import (
	"context"

	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"go.temporal.io/sdk/workflow"
)

func (a Activities) VoidHold(ctx context.Context, id string) error {
	_, err := a.client.WalletsApi.
		VoidHold(ctx, id).
		Execute()
	if err != nil {
		return formance.ExtractOpenAPIErrorMessage(err)
	}
	return nil
}

var VoidHoldActivity = Activities{}.VoidHold

func VoidHold(ctx workflow.Context, id string) error {
	if err := workflow.ExecuteActivity(ctx, VoidHoldActivity, id).Get(ctx, nil); err != nil {
		return errors.Unwrap(err)
	}
	return nil
}
