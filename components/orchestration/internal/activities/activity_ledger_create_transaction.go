package activities

import (
	"context"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"go.temporal.io/sdk/workflow"
)

func (a Activities) CreateTransaction(ctx context.Context, ledger string, data sdk.PostTransaction) (*sdk.TransactionsResponse, error) {
	ret, _, err := a.client.TransactionsApi.
		CreateTransaction(ctx, ledger).
		PostTransaction(data).
		Execute()
	if err != nil {
		return nil, sdk.ExtractOpenAPIErrorMessage(err)
	}
	return ret, nil
}

var CreateTransactionActivity = Activities{}.CreateTransaction

func CreateTransaction(ctx workflow.Context, ledger string, request sdk.PostTransaction) (*sdk.Transaction, error) {
	tx := &sdk.TransactionsResponse{}
	if err := workflow.ExecuteActivity(ctx, CreateTransactionActivity, ledger, request).Get(ctx, tx); err != nil {
		return nil, errors.Unwrap(err)
	}
	return &tx.Data[0], nil
}
