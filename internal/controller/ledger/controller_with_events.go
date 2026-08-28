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
	ledger      ledger.Ledger
	listener    Listener
	eventBuffer *transactionalEventBuffer
}

// transactionalEventBuffer follows transaction boundaries independently from
// the controller wrappers created by BeginTX and LockLedger. A nested commit
// promotes its events to the enclosing transaction; only the root commit
// publishes them.
type transactionalEventBuffer struct {
	parent    *transactionalEventBuffer
	callbacks []func()
}

func NewControllerWithEvents(ledger ledger.Ledger, underlying Controller, listener Listener) *ControllerWithEvents {
	return &ControllerWithEvents{
		Controller: underlying,
		ledger:     ledger,
		listener:   listener,
	}
}

func (c *ControllerWithEvents) handleEvent(ctx context.Context, fn func()) {
	if c.eventBuffer == nil {
		fn()
		return
	}

	c.eventBuffer.add(fn)
}

func (b *transactionalEventBuffer) add(fn func()) {
	b.callbacks = append(b.callbacks, fn)
}

func (b *transactionalEventBuffer) commit() {
	if b.parent != nil {
		b.parent.callbacks = append(b.parent.callbacks, b.callbacks...)
	} else {
		for _, callback := range b.callbacks {
			callback()
		}
	}
	b.callbacks = nil
}

func (b *transactionalEventBuffer) rollback() {
	b.callbacks = nil
}

func (c *ControllerWithEvents) CreateTransaction(ctx context.Context, parameters Parameters[CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
	log, ret, idempotencyHit, err := c.Controller.CreateTransaction(ctx, parameters)
	if err != nil {
		return nil, nil, false, err
	}
	if !parameters.DryRun {
		c.handleEvent(ctx, func() {
			c.listener.CommittedTransactions(ctx, c.ledger.Name, ret.Transaction, ret.AccountMetadata)
		})
	}

	return log, ret, idempotencyHit, nil
}

func (c *ControllerWithEvents) RevertTransaction(ctx context.Context, parameters Parameters[RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, bool, error) {
	log, ret, idempotencyHit, err := c.Controller.RevertTransaction(ctx, parameters)
	if err != nil {
		return nil, nil, false, err
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

	return log, ret, idempotencyHit, nil
}

func (c *ControllerWithEvents) SaveTransactionMetadata(ctx context.Context, parameters Parameters[SaveTransactionMetadata]) (*ledger.Log, bool, error) {
	log, idempotencyHit, err := c.Controller.SaveTransactionMetadata(ctx, parameters)
	if err != nil {
		return nil, false, err
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

	return log, idempotencyHit, nil
}

func (c *ControllerWithEvents) SaveAccountMetadata(ctx context.Context, parameters Parameters[SaveAccountMetadata]) (*ledger.Log, bool, error) {
	log, idempotencyHit, err := c.Controller.SaveAccountMetadata(ctx, parameters)
	if err != nil {
		return nil, false, err
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

	return log, idempotencyHit, nil
}

func (c *ControllerWithEvents) DeleteTransactionMetadata(ctx context.Context, parameters Parameters[DeleteTransactionMetadata]) (*ledger.Log, bool, error) {
	log, idempotencyHit, err := c.Controller.DeleteTransactionMetadata(ctx, parameters)
	if err != nil {
		return nil, false, err
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

	return log, idempotencyHit, nil
}

func (c *ControllerWithEvents) DeleteAccountMetadata(ctx context.Context, parameters Parameters[DeleteAccountMetadata]) (*ledger.Log, bool, error) {
	log, idempotencyHit, err := c.Controller.DeleteAccountMetadata(ctx, parameters)
	if err != nil {
		return nil, false, err
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

	return log, idempotencyHit, nil
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
		eventBuffer: &transactionalEventBuffer{
			parent: c.eventBuffer,
		},
	}, tx, nil
}

func (c *ControllerWithEvents) LockLedger(ctx context.Context) (Controller, bun.IDB, func() error, error) {
	ctrl, db, release, err := c.Controller.LockLedger(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	// Locking swaps the underlying controller but does not create a transaction
	// boundary, so both wrappers share the same event buffer.
	return &ControllerWithEvents{
		ledger:      c.ledger,
		Controller:  ctrl,
		listener:    c.listener,
		eventBuffer: c.eventBuffer,
	}, db, release, nil
}

func (c *ControllerWithEvents) Commit(ctx context.Context) error {
	err := c.Controller.Commit(ctx)
	if err != nil {
		return err
	}

	if c.eventBuffer != nil {
		c.eventBuffer.commit()
		c.eventBuffer = nil
	}

	return nil
}

func (c *ControllerWithEvents) Rollback(ctx context.Context) error {
	if c.eventBuffer != nil {
		c.eventBuffer.rollback()
		c.eventBuffer = nil
	}

	return c.Controller.Rollback(ctx)
}

var _ Controller = (*ControllerWithEvents)(nil)
