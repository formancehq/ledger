package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/query"

	ledger "github.com/formancehq/ledger/internal"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/pkg/features"
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
	tracer      trace.Tracer
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

	ctx, span := r.tracer.Start(ctx, "Run")
	defer span.End()

	initialQuery := storagecommon.InitialPaginatedQuery[systemstore.ListLedgersQueryPayload]{
		Options: storagecommon.ResourceQuery[systemstore.ListLedgersQueryPayload]{
			Builder: query.Match(fmt.Sprintf("features[%s]", features.FeatureHashLogs), "ASYNC"),
		},
	}
	systemStore := systemstore.New(r.db)
	return storagecommon.Iterate(
		ctx,
		initialQuery,
		systemStore.Ledgers().Paginate,
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
	ctx, span := r.tracer.Start(ctx, "RunForLedger")
	defer span.End()

	span.SetAttributes(attribute.String("ledger", l.Name))

	var err error
	_, err = r.db.NewRaw(fmt.Sprintf(`
			call "%s".create_blocks(?, ?)
		`, l.Bucket), l.Name, r.cfg.MaxBlockSize).
		Exec(ctx)
	return err
}

func NewAsyncBlockRunner(logger logging.Logger, db *bun.DB, cfg AsyncBlockRunnerConfig, opts ...Option) *AsyncBlockRunner {
	ret := &AsyncBlockRunner{
		stopChannel: make(chan chan struct{}),
		logger:      logger,
		db:          db,
		cfg:         cfg,
	}

	for _, opt := range append(defaultOptions, opts...) {
		opt(ret)
	}

	return ret
}

type Option func(*AsyncBlockRunner)

func WithTracer(tracer trace.Tracer) Option {
	return func(r *AsyncBlockRunner) {
		r.tracer = tracer
	}
}

var defaultOptions = []Option{
	WithTracer(noop.Tracer{}),
}

func NewAsyncBlockRunnerModule(cfg AsyncBlockRunnerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger, db *bun.DB) (*AsyncBlockRunner, error) {
			return NewAsyncBlockRunner(logger, db, cfg), nil
		}),
		fx.Invoke(func(lc fx.Lifecycle, asyncBlockRunner *AsyncBlockRunner) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := asyncBlockRunner.Run(context.WithoutCancel(ctx)); err != nil {
							panic(err)
						}
					}()

					return nil
				},
				OnStop: asyncBlockRunner.Stop,
			})
		}),
	)
}
