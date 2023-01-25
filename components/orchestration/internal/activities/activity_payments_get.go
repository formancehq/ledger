package activities

import (
	"context"
	"errors"
	"net/http"

	sdk "github.com/formancehq/formance-sdk-go"
	"go.temporal.io/sdk/workflow"
)

func (a Activities) GetPayment(ctx context.Context, id string) (*sdk.PaymentResponse, error) {
	ret, httpResponse, err := a.client.PaymentsApi.
		GetPayment(ctx, id).
		Execute()
	if err != nil {
		switch httpResponse.StatusCode {
		case http.StatusNotFound:
			return nil, errors.New("payment not found")
		default:
			return nil, sdk.ExtractOpenAPIErrorMessage(err)
		}
	}
	return ret, nil
}

var GetPaymentActivity = Activities{}.GetPayment

func GetPayment(ctx workflow.Context, id string) (*sdk.Payment, error) {
	ret := &sdk.PaymentResponse{}
	if err := workflow.ExecuteActivity(ctx, GetPaymentActivity, id).Get(ctx, ret); err != nil {
		return nil, errors.Unwrap(err)
	}
	return &ret.Data, nil
}
