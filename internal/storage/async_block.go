package storage

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/leadership"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/robfig/cron/v3"
	"github.com/uptrace/bun"
	"time"
)

type Store interface {
	ListLedgers(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error)
}

type AsyncBlockRunner struct {
	maxBlockSize int
	stopChannel  chan chan struct{}
	store        Store
	logger       logging.Logger
	schedule     cron.Schedule
}

func (r *AsyncBlockRunner) Run(ctx context.Context, db *leadership.DatabaseHandle) {
	now := time.Now()
	next := r.schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx, db); err != nil {
				r.logger.Errorf("error running block runner: %v", err)
			}

			now = time.Now()
			next = r.schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return
		}
	}
}

func (r *AsyncBlockRunner) Stop(ctx context.Context) error {
	ch := make(chan struct{})
	select {
	case <-ctx.Done():
		return ctx.Err()
	case r.stopChannel <- ch:
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ch:
		}
	}
	return nil
}

func (r *AsyncBlockRunner) run(ctx context.Context, db *leadership.DatabaseHandle) error {
	initialQuery := ledgercontroller.NewListLedgersQuery(10)
	initialQuery.Options.Options.Features = map[string]string{
		features.FeatureHashLogs: "ASYNC",
	}
	return bunpaginate.Iterate(
		ctx,
		initialQuery,
		func(ctx context.Context, q ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error) {
			return r.store.ListLedgers(ctx, q)
		},
		func(cursor *bunpaginate.Cursor[ledger.Ledger]) error {
			for _, l := range cursor.Data {
				if err := r.processLedger(ctx, db, l); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func (r *AsyncBlockRunner) processLedger(ctx context.Context, dbHandle *leadership.DatabaseHandle, l ledger.Ledger) error {
	var err error
	dbHandle.Exec(func(db bun.IDB) {
		_, err = db.NewRaw(fmt.Sprintf(`
			call "%s".create_blocks(?, ?)
		`, l.Bucket), l.Name, r.maxBlockSize).
			Exec(ctx)
	})
	return err
}

func NewAsyncBlockRunner(store Store, logger logging.Logger, schedule cron.Schedule, maxBlockSize int) *AsyncBlockRunner {
	return &AsyncBlockRunner{
		maxBlockSize: maxBlockSize,
		schedule:     schedule,
		stopChannel:  make(chan chan struct{}),
		store:        store,
		logger:       logger,
	}
}
