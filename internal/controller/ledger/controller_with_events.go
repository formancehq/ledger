package ledger

import (
	"context"
	"fmt"
	ledger "github.com/formancehq/ledger/internal"
)

type ControllerWithEvents struct {
	Controller
	ledger   ledger.Ledger
	listener Listener
}

func NewControllerWithEvents(ledger ledger.Ledger, underlying Controller, listener Listener) *ControllerWithEvents {
	return &ControllerWithEvents{
		Controller: underlying,
		ledger:     ledger,
		listener:   listener,
	}
}
func (ctrl *ControllerWithEvents) CreateTransaction(ctx context.Context, parameters Parameters[RunScript]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	log, ret, err := ctrl.Controller.CreateTransaction(ctx, parameters)
	if err != nil {
		return nil, nil, err
	}
	if !parameters.DryRun {
		ctrl.listener.CommittedTransactions(ctx, ctrl.ledger.Name, ret.Transaction, ret.AccountMetadata)
	}

	return log, ret, nil
}

func (ctrl *ControllerWithEvents) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	log, ret, err := ctrl.Controller.RevertTransaction(ctx, parameters)
	if err != nil {
		return nil, nil, err
	}
	if !parameters.DryRun {
		ctrl.listener.RevertedTransaction(
			ctx,
			ctrl.ledger.Name,
			ret.RevertedTransaction,
			ret.RevertedTransaction,
		)
	}

	return log, ret, nil
}

func (ctrl *ControllerWithEvents) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	log, err := ctrl.Controller.SaveTransactionMetadata(ctx, parameters)
	if err != nil {
		return nil, err
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

	return log, nil
}

func (ctrl *ControllerWithEvents) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	log, err := ctrl.Controller.SaveAccountMetadata(ctx, parameters)
	if err != nil {
		return nil, err
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

	return log, nil
}

func (ctrl *ControllerWithEvents) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	log, err := ctrl.Controller.DeleteTransactionMetadata(ctx, parameters)
	if err != nil {
		return nil, err
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

	return log, nil
}

func (ctrl *ControllerWithEvents) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	log, err := ctrl.Controller.DeleteAccountMetadata(ctx, parameters)
	if err != nil {
		return nil, err
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

	return log, nil
}

var _ Controller = (*ControllerWithEvents)(nil)
