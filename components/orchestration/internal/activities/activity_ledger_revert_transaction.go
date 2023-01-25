package activities

import (
	"context"

	sdk "github.com/formancehq/formance-sdk-go"
	"github.com/pkg/errors"
	"go.temporal.io/sdk/workflow"
)

func (a Activities) RevertTransaction(ctx context.Context, ledger string, txID int64) (*sdk.Transaction, error) {
	ret, _, err := a.client.TransactionsApi.
		RevertTransaction(ctx, ledger, txID).
		Execute()
	if err != nil {
		return nil, sdk.ExtractOpenAPIErrorMessage(err)
	}
	return &ret.Data, nil
}

var RevertTransactionActivity = Activities{}.RevertTransaction

func RevertTransaction(ctx workflow.Context, ledger string, txID int64) (*sdk.Transaction, error) {
	tx := &sdk.Transaction{}
	if err := workflow.ExecuteActivity(ctx, RevertTransactionActivity, ledger, txID).Get(ctx, tx); err != nil {
		return nil, errors.Unwrap(err)
	}
	return tx, nil
}
