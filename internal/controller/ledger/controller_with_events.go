package ledger

import (
	"context"
	"fmt"
	ledger "github.com/formancehq/ledger/internal"
)

type ControllerWithEvents struct {
	Controller
	ledger ledger.Ledger
	listener       Listener
}

func NewControllerWithEvents(ledger ledger.Ledger, underlying Controller, listener Listener) *ControllerWithEvents {
	ret := &ControllerWithEvents{
		Controller: underlying,
		ledger: ledger,
		listener:   listener,
	}
	return ret
}
func (ctrl *ControllerWithEvents) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*CreateTransactionResult, error) {
	ret, err := ctrl.Controller.CreateTransaction(ctx, parameters)
	if err != nil {
		return nil, err
	}
	if !parameters.DryRun {
		ctrl.listener.CommittedTransactions(ctx, ctrl.ledger.Name, ret.Transaction, ret.AccountMetadata)
	}

	return ret, nil
}

func (ctrl *ControllerWithEvents) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*RevertTransactionResult, error) {
	ret, err := ctrl.Controller.RevertTransaction(ctx, parameters)
	if err != nil {
		return nil, err
	}
	if !parameters.DryRun {
		ctrl.listener.RevertedTransaction(
			ctx,
			ctrl.ledger.Name,
			ret.RevertedTransaction,
			ret.RevertedTransaction,
		)
	}

	return ret, nil
}

func (ctrl *ControllerWithEvents) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) error {
	err := ctrl.Controller.SaveTransactionMetadata(ctx, parameters)
	if err != nil {
		return err
	}
	if !parameters.DryRun {
		ctrl.listener.SavedMetadata(
			ctx,
			ctrl.ledger.Name,
			ledger.MetaTargetTypeTransaction,
			fmt.Sprint(parameters.Input.TransactionID),
			parameters.Input.Metadata,
		)
	}

	return nil
}

func (ctrl *ControllerWithEvents) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) error {
	err := ctrl.Controller.SaveAccountMetadata(ctx, parameters)
	if err != nil {
		return err
	}
	if !parameters.DryRun {
		ctrl.listener.SavedMetadata(
			ctx,
			ctrl.ledger.Name,
			ledger.MetaTargetTypeAccount,
			parameters.Input.Address,
			parameters.Input.Metadata,
		)
	}

	return nil
}

func (ctrl *ControllerWithEvents) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) error {
	err := ctrl.Controller.DeleteTransactionMetadata(ctx, parameters)
	if err != nil {
		return err
	}
	if !parameters.DryRun {
		ctrl.listener.DeletedMetadata(
			ctx,
			ctrl.ledger.Name,
			ledger.MetaTargetTypeTransaction,
			fmt.Sprint(parameters.Input.TransactionID),
			parameters.Input.Key,
		)
	}

	return nil
}

func (ctrl *ControllerWithEvents) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) error {
	err := ctrl.Controller.DeleteAccountMetadata(ctx, parameters)
	if err != nil {
		return err
	}
	if !parameters.DryRun {
		ctrl.listener.DeletedMetadata(
			ctx,
			ctrl.ledger.Name,
			ledger.MetaTargetTypeAccount,
			parameters.Input.Address,
			parameters.Input.Key,
		)
	}

	return nil
}

var _ Controller = (*ControllerWithEvents)(nil)
