package ledger

import (
	"context"
	"database/sql"
	"fmt"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
)

type ControllerWithEvents struct {
	Controller
	ledger   ledger.Ledger
	listener Listener
	atCommit []func()
	parent   *ControllerWithEvents
	hasTx    bool
}

func NewControllerWithEvents(ledger ledger.Ledger, underlying Controller, listener Listener) *ControllerWithEvents {
	return &ControllerWithEvents{
		Controller: underlying,
		ledger:     ledger,
		listener:   listener,
	}
}

func (c *ControllerWithEvents) handleEvent(ctx context.Context, fn func()) {
	if !c.hasTx {
		fn()
		return
	}
	if c.parent != nil && c.parent.hasTx {
		c.parent.handleEvent(ctx, fn)
		return
	}

	c.atCommit = append(c.atCommit, fn)
}

func (c *ControllerWithEvents) CreateTransaction(ctx context.Context, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, error) {
	log, ret, err := c.Controller.CreateTransaction(ctx, parameters)
	if err != nil {
		return nil, nil, err
	}
	if !parameters.DryRun {
		c.handleEvent(ctx, func() {
			c.listener.CommittedTransactions(ctx, c.ledger.Name, ret.Transaction, ret.AccountMetadata)
		})
	}

	return log, ret, nil
}

func (c *ControllerWithEvents) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, error) {
	log, ret, err := c.Controller.RevertTransaction(ctx, parameters)
	if err != nil {
		return nil, nil, err
	}
	if !parameters.DryRun {
		c.handleEvent(ctx, func() {
			c.listener.RevertedTransaction(
				ctx,
				c.ledger.Name,
				ret.RevertedTransaction,
				ret.RevertTransaction,
			)
		})
	}

	return log, ret, nil
}

func (c *ControllerWithEvents) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, error) {
	log, err := c.Controller.SaveTransactionMetadata(ctx, parameters)
	if err != nil {
		return nil, err
	}
	if !parameters.DryRun {
		c.handleEvent(ctx, func() {
			c.listener.SavedMetadata(
				ctx,
				c.ledger.Name,
				ledger.MetaTargetTypeTransaction,
				fmt.Sprint(parameters.Input.TransactionID),
				parameters.Input.Metadata,
			)
		})
	}

	return log, nil
}

func (c *ControllerWithEvents) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, error) {
	log, err := c.Controller.SaveAccountMetadata(ctx, parameters)
	if err != nil {
		return nil, err
	}
	if !parameters.DryRun {
		c.handleEvent(ctx, func() {
			c.listener.SavedMetadata(
				ctx,
				c.ledger.Name,
				ledger.MetaTargetTypeAccount,
				parameters.Input.Address,
				parameters.Input.Metadata,
			)
		})
	}

	return log, nil
}

func (c *ControllerWithEvents) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, error) {
	log, err := c.Controller.DeleteTransactionMetadata(ctx, parameters)
	if err != nil {
		return nil, err
	}
	if !parameters.DryRun {
		c.handleEvent(ctx, func() {
			c.listener.DeletedMetadata(
				ctx,
				c.ledger.Name,
				ledger.MetaTargetTypeTransaction,
				fmt.Sprint(parameters.Input.TransactionID),
				parameters.Input.Key,
			)
		})
	}

	return log, nil
}

func (c *ControllerWithEvents) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, error) {
	log, err := c.Controller.DeleteAccountMetadata(ctx, parameters)
	if err != nil {
		return nil, err
	}
	if !parameters.DryRun {
		c.handleEvent(ctx, func() {
			c.listener.DeletedMetadata(
				ctx,
				c.ledger.Name,
				ledger.MetaTargetTypeAccount,
				parameters.Input.Address,
				parameters.Input.Key,
			)
		})
	}

	return log, nil
}

func (c *ControllerWithEvents) BeginTX(ctx context.Context, options *sql.TxOptions) (Controller, *bun.Tx, error) {
	ctrl, tx, err := c.Controller.BeginTX(ctx, options)
	if err != nil {
		return nil, nil, err
	}

	return &ControllerWithEvents{
		ledger:     c.ledger,
		Controller: ctrl,
		listener:   c.listener,
		parent:     c,
		hasTx:      true,
	}, tx, nil
}

func (c *ControllerWithEvents) LockLedger(ctx context.Context) (Controller, bun.IDB, func() error, error) {
	ctrl, db, release, err := c.Controller.LockLedger(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	return &ControllerWithEvents{
		ledger:     c.ledger,
		Controller: ctrl,
		listener:   c.listener,
		parent:     c,
	}, db, release, nil
}

func (c *ControllerWithEvents) Commit(ctx context.Context) error {
	err := c.Controller.Commit(ctx)
	if err != nil {
		return err
	}

	for _, f := range c.atCommit {
		f()
	}

	return nil
}

func (c *ControllerWithEvents) Rollback(ctx context.Context) error {
	c.atCommit = nil

	return c.Controller.Rollback(ctx)
}

var _ Controller = (*ControllerWithEvents)(nil)
