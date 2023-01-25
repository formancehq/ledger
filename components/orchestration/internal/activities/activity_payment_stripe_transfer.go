package activities

import (
	"context"

	"github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"go.temporal.io/sdk/workflow"
)

func (a Activities) StripeTransfer(ctx context.Context, request formance.StripeTransferRequest) error {
	_, _, err := a.client.PaymentsApi.
		ConnectorsStripeTransfer(ctx).
		StripeTransferRequest(request).
		Execute()
	if err != nil {
		return formance.ExtractOpenAPIErrorMessage(err)
	}
	return nil
}

var StripeTransferActivity = Activities{}.StripeTransfer

func StripeTransfer(ctx workflow.Context, request formance.StripeTransferRequest) error {
	if err := workflow.ExecuteActivity(ctx, StripeTransferActivity, request).Get(ctx, nil); err != nil {
		return errors.Unwrap(err)
	}
	return nil
}
