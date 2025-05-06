package cmd

import (
	"context"
	"fmt"
	"github.com/formancehq/ledger/pkg/client"
	"github.com/formancehq/ledger/pkg/client/models/components"
	"github.com/formancehq/ledger/pkg/client/models/operations"
	"github.com/formancehq/ledger/pkg/client/models/sdkerrors"
	"math/big"
)

func runWorkflow(ctx context.Context, client *client.Formance, events chan any) error {

	const ledgerName = "testing"

	_, err := client.Ledger.V2.CreateLedger(ctx, operations.V2CreateLedgerRequest{
		Ledger:                ledgerName,
		V2CreateLedgerRequest: components.V2CreateLedgerRequest{},
	})
	if err != nil {
		switch err := err.(type) {
		case *sdkerrors.V2ErrorResponse:
			if err.ErrorCode != components.V2ErrorsEnumLedgerAlreadyExists {
				return fmt.Errorf("failed to create ledger, got api error: %w", err)
			}
		default:
			return fmt.Errorf("failed to create ledger: %w", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			_, err := client.Ledger.V2.CreateTransaction(ctx, operations.V2CreateTransactionRequest{
				Ledger: ledgerName,
				V2PostTransaction: components.V2PostTransaction{
					Postings: []components.V2Posting{
						{
							Source:      "world",
							Destination: "bank",
							Asset:       "USD/2",
							Amount:      big.NewInt(100),
						},
					},
				},
			})
			if err != nil {
				return err
			}

			events <- struct{}{}
		}
	}
}

