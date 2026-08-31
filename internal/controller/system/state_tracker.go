package system

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/otlp"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
	"sync"
)

type controllerFacade struct {
	ledgercontroller.Controller
	state             *ledgerState
	tx                *bun.Tx
	parent            *controllerFacade
	stateTransitioned bool
}

type ledgerState struct {
	mu     sync.RWMutex
	ledger ledger.Ledger
}

func (c *controllerFacade) handleState(ctx context.Context, dryRun bool, fn func(ctrl ledgercontroller.Controller) error) error {
	c.state.mu.RLock()
	l := c.state.ledger
	c.state.mu.RUnlock()

	if l.State == ledger.StateInUse {
		return fn(c.Controller)
	}

	if c.tx != nil && !dryRun {
		return c.handleStateInTransaction(ctx, l, fn)
	}

	// Dry runs still need the post-import sequence reset. Use a dedicated
	// transaction (or a nested savepoint) so the state transition is rolled back
	// while PostgreSQL keeps the non-transactional sequence update.
	ctrl, _, err := c.beginTX(ctx, nil)
	if err != nil {
		return err
	}
	defer func() {
		_ = ctrl.Rollback(ctx)
	}()

	if err := ctrl.handleStateInTransaction(ctx, l, fn); err != nil {
		return err
	}

	if !dryRun {
		if err := ctrl.Commit(ctx); err != nil {
			return fmt.Errorf("failed to commit transaction: %w", err)
		}
	} else {
		if err := ctrl.Rollback(ctx); err != nil {
			return fmt.Errorf("failed to rollback transaction: %w", err)
		}
	}

	return nil
}

func (c *controllerFacade) handleStateInTransaction(ctx context.Context, l ledger.Ledger, fn func(ctrl ledgercontroller.Controller) error) error {
	if c.stateTransitioned {
		return fn(c.Controller)
	}

	return withLock(ctx, c.Controller, func(ctrl ledgercontroller.Controller, conn bun.IDB) error {

		// todo: remove that in a later version
		ret, err := c.tx.NewUpdate().
			Model(&l).
			Set("state = ?", ledger.StateInUse).
			Where("id = ? and state = ?", l.ID, ledger.StateInitializing).
			Exec(ctx)
		if err != nil {
			return err
		}

		rowsAffected, err := ret.RowsAffected()
		if err != nil {
			return err
		}

		if rowsAffected > 0 {
			_, err := c.tx.NewRaw(
				fmt.Sprintf(`
					select setval(
						'"%s"."transaction_id_%d"',
						coalesce((
							select max(id) + 1 from "%s".transactions where ledger = '%s'
						), 1)::bigint,
						false
					)
				`, l.Bucket, l.ID, l.Bucket, l.Name),
			).Exec(ctx)
			if err != nil {
				return fmt.Errorf("failed to update transactions sequence value: %w", err)
			}

			_, err = c.tx.NewRaw(
				fmt.Sprintf(`
					select setval(
						'"%s"."log_id_%d"',
						coalesce((
							select max(id) + 1 from "%s".logs where ledger = '%s'
						), 1)::bigint,
						false
					)
				`, l.Bucket, l.ID, l.Bucket, l.Name),
			).Exec(ctx)
			if err != nil {
				return fmt.Errorf("failed to update logs sequence value: %w", err)
			}
		}

		if err := fn(ctrl); err != nil {
			return err
		}

		c.stateTransitioned = true
		return nil
	})
}

func (c *controllerFacade) beginTX(ctx context.Context, options *sql.TxOptions) (*controllerFacade, *bun.Tx, error) {
	ctrl, tx, err := c.Controller.BeginTX(ctx, options)
	if err != nil {
		return nil, nil, err
	}

	return &controllerFacade{
		Controller:        ctrl,
		state:             c.state,
		tx:                tx,
		parent:            c,
		stateTransitioned: c.stateTransitioned,
	}, tx, nil
}

func (c *controllerFacade) BeginTX(ctx context.Context, options *sql.TxOptions) (ledgercontroller.Controller, *bun.Tx, error) {
	// Keep the state tracker around the transactional controller so atomic bulks
	// cannot bypass the initializing-to-in-use transition and sequence reset.
	return c.beginTX(ctx, options)
}

func (c *controllerFacade) Commit(ctx context.Context) error {
	if err := c.Controller.Commit(ctx); err != nil {
		return err
	}

	if c.stateTransitioned {
		if c.parent != nil && c.parent.tx != nil {
			// Committing a nested transaction only releases its savepoint. Defer
			// publishing the state transition until the root transaction commits.
			c.parent.stateTransitioned = true
		} else {
			c.state.mu.Lock()
			c.state.ledger.State = ledger.StateInUse
			c.state.mu.Unlock()
		}
	}


	return nil
}

func (c *controllerFacade) Rollback(ctx context.Context) error {
	return c.Controller.Rollback(ctx)
}

func (c *controllerFacade) CreateTransaction(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.CreateTransaction]) (*ledger.Log, *ledger.CreatedTransaction, bool, error) {
	var (
		log            *ledger.Log
		ret            *ledger.CreatedTransaction
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, ret, idempotencyHit, err = ctrl.CreateTransaction(ctx, parameters)
		return err
	})

	return log, ret, idempotencyHit, err
}

func (c *controllerFacade) RevertTransaction(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.RevertTransaction]) (*ledger.Log, *ledger.RevertedTransaction, bool, error) {
	var (
		log            *ledger.Log
		ret            *ledger.RevertedTransaction
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, ret, idempotencyHit, err = ctrl.RevertTransaction(ctx, parameters)
		return err
	})

	return log, ret, idempotencyHit, err
}

func (c *controllerFacade) SaveTransactionMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.SaveTransactionMetadata]) (*ledger.Log, bool, error) {
	var (
		log            *ledger.Log
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, idempotencyHit, err = ctrl.SaveTransactionMetadata(ctx, parameters)
		return err
	})

	return log, idempotencyHit, err
}

func (c *controllerFacade) SaveAccountMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.SaveAccountMetadata]) (*ledger.Log, bool, error) {
	var (
		log            *ledger.Log
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, idempotencyHit, err = ctrl.SaveAccountMetadata(ctx, parameters)
		return err
	})

	return log, idempotencyHit, err
}

func (c *controllerFacade) DeleteTransactionMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.DeleteTransactionMetadata]) (*ledger.Log, bool, error) {
	var (
		log            *ledger.Log
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, idempotencyHit, err = ctrl.DeleteTransactionMetadata(ctx, parameters)
		return err
	})

	return log, idempotencyHit, err
}

func (c *controllerFacade) DeleteAccountMetadata(ctx context.Context, parameters ledgercontroller.Parameters[ledgercontroller.DeleteAccountMetadata]) (*ledger.Log, bool, error) {
	var (
		log            *ledger.Log
		idempotencyHit bool
		err            error
	)
	err = c.handleState(ctx, parameters.DryRun, func(ctrl ledgercontroller.Controller) error {
		log, idempotencyHit, err = ctrl.DeleteAccountMetadata(ctx, parameters)
		return err
	})
	return log, idempotencyHit, err
}

func (c *controllerFacade) Import(ctx context.Context, stream chan ledger.Log) error {
	c.state.mu.RLock()
	l := c.state.ledger
	c.state.mu.RUnlock()

	return withLock(ctx, c.Controller, func(ctrl ledgercontroller.Controller, conn bun.IDB) error {
		// todo: remove that in a later version
		if err := conn.NewSelect().Model(&l).
			Where("id = ?", l.ID).
			Scan(ctx); err != nil {
			return err
		}

		if l.State != ledger.StateInitializing {
			return ledgercontroller.NewErrImport(errors.New("ledger is not in initializing state"))
		}

		return ctrl.Import(ctx, stream)
	})
}

var _ ledgercontroller.Controller = (*controllerFacade)(nil)

func newLedgerStateTracker(ctrl ledgercontroller.Controller, ledger ledger.Ledger) ledgercontroller.Controller {
	return &controllerFacade{
		Controller: ctrl,
		state: &ledgerState{
			ledger: ledger,
		},
	}
}

func withLock(ctx context.Context, ctrl ledgercontroller.Controller, fn func(ctrl ledgercontroller.Controller, conn bun.IDB) error) error {
	lockedCtrl, conn, release, err := ctrl.LockLedger(ctx)
	if err != nil {
		return fmt.Errorf("failed to lock ledger: %w", err)
	}

	defer func() {
		if err := release(); err != nil {
			logging.FromContext(ctx).Errorf(
				"failed to release lock: %v",
				err,
			)
			otlp.RecordError(ctx, fmt.Errorf("failed to release lock: %v", err))
		}
	}()

	return fn(lockedCtrl, conn)
}
