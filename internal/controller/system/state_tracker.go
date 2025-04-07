package system

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v3/logging"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
)

type controllerFacade struct {
	ledgercontroller.Controller
	ledger ledger.Ledger
}

func (c *controllerFacade) handleState(ctx context.Context, dryRun bool, fn func(ctrl ledgercontroller.Controller) error) error {
	if dryRun || c.ledger.State == ledger.StateInUse {
		return fn(c.Controller)
	}

	ctrl, tx, err := c.Controller.BeginTX(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = ctrl.Rollback(ctx)
	}()

	if err := fn(ctrl); err != nil {
		return err
	}

	c.ledger.State = ledger.StateInUse

	_, err = tx.NewUpdate().
		Model(&c.ledger).
		Set("state = ?", c.ledger.State).
		Where("id = ?", c.ledger.ID).
		Exec(ctx)
	if err != nil {
		return err
	}

	return ctrl.Commit(ctx)
}

func (c *controllerFacade) CreateTransaction(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	var (
		log *ledger.Log
		ret *ledger.CreatedTransaction
		err error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, ret, err = ctrl.CreateTransaction(ctx, parameters)
		return err
	})

	return log, ret, err
}

func (c *controllerFacade) RevertTransaction(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	var (
		log *ledger.Log
		ret *ledger.RevertedTransaction
		err error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, ret, err = ctrl.RevertTransaction(ctx, parameters)
		return err
	})

	return log, ret, err
}

func (c *controllerFacade) SaveTransactionMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.SaveTransactionMetadata]) (*ledger.Log, error) {
	var (
		log *ledger.Log
		err error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, err = ctrl.SaveTransactionMetadata(ctx, parameters)
		return err
	})

	return log, err
}

func (c *controllerFacade) SaveAccountMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]) (*ledger.Log, error) {
	var (
		log *ledger.Log
		err error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, err = ctrl.SaveAccountMetadata(ctx, parameters)
		return err
	})

	return log, err
}

func (c *controllerFacade) DeleteTransactionMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.DeleteTransactionMetadata]) (*ledger.Log, error) {
	var (
		log *ledger.Log
		err error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, err = ctrl.DeleteTransactionMetadata(ctx, parameters)
		return err
	})

	return log, err
}

func (c *controllerFacade) DeleteAccountMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.DeleteAccountMetadata]) (*ledger.Log, error) {
	var (
		log *ledger.Log
		err error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, err = ctrl.DeleteAccountMetadata(ctx, parameters)
		return err
	})
	return log, err
}

func (c *controllerFacade) Import(ctx context.Context, stream chan ledger.Log) error {
	// Check before open a sql transaction
	if c.ledger.State != ledger.StateInitializing {
		return ledgercontroller.NewErrImport(errors.New("ledger is not in initializing state"))
	}

	// Use serializable isolation level to ensure no concurrent request use the store.
	// If a concurrent transactions is made while we are importing some logs, the transaction importing logs will
	// be canceled with serialization error.
	ctrl, tx, err := c.BeginTX(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	if err != nil {
		return fmt.Errorf("failed to start transaction: %w", err)
	}

	if err := tx.NewSelect().Model(&c.ledger).
		Where("id = ?", c.ledger.ID).
		Scan(ctx); err != nil {
		return err
	}

	// Check again after the transaction is open
	if c.ledger.State != ledger.StateInitializing {
		return ledgercontroller.NewErrImport(errors.New("ledger is not in initializing state"))
	}

	if err := ctrl.Import(ctx, stream); err != nil {
		if rollbackErr := ctrl.Rollback(ctx); rollbackErr != nil {
			logging.FromContext(ctx).Errorf("failed to rollback transaction: %v", rollbackErr)
		}
		return err
	}

	if err := ctrl.Commit(ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

var _ ledgercontroller.Controller = (*controllerFacade)(nil)

func newLedgerStateTracker(ctrl ledgercontroller.Controller, ledger ledger.Ledger) ledgercontroller.Controller {
	return &controllerFacade{
		Controller: ctrl,
		ledger:     ledger,
	}
}
