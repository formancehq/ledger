package worker

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/resources"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/robfig/cron/v3"
	"github.com/uptrace/bun"
	"time"
)

type AsyncBlockRunnerConfig struct {
	MaxBlockSize int
	Schedule     cron.Schedule
}

type AsyncBlockRunner struct {
	stopChannel chan chan struct{}
	logger      logging.Logger
	db          *bun.DB
	cfg         AsyncBlockRunnerConfig
}

func (r *AsyncBlockRunner) Name() string {
	return "Async block hasher"
}

func (r *AsyncBlockRunner) Run(ctx context.Context) error {

	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx); err != nil {
				r.logger.Errorf("error running block runner: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
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

func (r *AsyncBlockRunner) run(ctx context.Context) error {
	initialQuery := ledgercontroller.NewListLedgersQuery(10)
	initialQuery.Options.Builder = query.Match(fmt.Sprintf("features[%s]", features.FeatureHashLogs), "ASYNC")
	systemStore := systemstore.New(r.db)
	return bunpaginate.Iterate(
		ctx,
		initialQuery,
		func(ctx context.Context, q resources.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Ledger], error) {
			return systemStore.Ledgers().Paginate(ctx, q)
		},
		func(cursor *bunpaginate.Cursor[ledger.Ledger]) error {
			for _, l := range cursor.Data {
				if err := r.processLedger(ctx, l); err != nil {
					return err
				}
			}
			return nil
		},
	)
}

func (r *AsyncBlockRunner) processLedger(ctx context.Context, l ledger.Ledger) error {
	var err error
	_, err = r.db.NewRaw(fmt.Sprintf(`
			call "%s".create_blocks(?, ?)
		`, l.Bucket), l.Name, r.cfg.MaxBlockSize).
		Exec(ctx)
	return err
}

func NewAsyncBlockRunner(logger logging.Logger, db *bun.DB, cfg AsyncBlockRunnerConfig) *AsyncBlockRunner {
	return &AsyncBlockRunner{
		stopChannel: make(chan chan struct{}),
		logger:      logger,
		db:          db,
		cfg:         cfg,
	}
}
