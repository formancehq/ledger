package storage

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/ledger/internal"
	storagecommon "github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
	"github.com/formancehq/ledger/internal/worker"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/robfig/cron/v3"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"time"
)

type AsyncBlockRunnerConfig struct {
	HashLogsBlockMaxSize  int    `mapstructure:"worker-async-block-hasher-max-block-size" description:"Max block size" default:"1000"`
	HashLogsBlockCRONSpec string `mapstructure:"worker-async-block-hasher-schedule" description:"Schedule" default:"0 * * * * *"`
}

type AsyncBlockRunner struct {
	stopChannel  chan chan struct{}
	logger       logging.Logger
	db           *bun.DB
	tracer       trace.Tracer
	maxBlockSize int
	schedule     cron.Schedule
}

func (r *AsyncBlockRunner) Name() string {
	return "Async block hasher"
}

func (r *AsyncBlockRunner) Run(ctx context.Context) error {

	now := time.Now()
	next := r.schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx); err != nil {
				r.logger.Errorf("error running block runner: %v", err)
			}

			now = time.Now()
			next = r.schedule.Next(now).Sub(now)
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

	initialQuery := storagecommon.InitialPaginatedQuery[any]{
		Options: storagecommon.ResourceQuery[any]{
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
		`, l.Bucket), l.Name, r.maxBlockSize).
		Exec(ctx)
	return err
}

func NewAsyncBlockRunner(
	logger logging.Logger,
	db *bun.DB,
	schedule cron.Schedule,
	maxBlockSize int,
	opts ...Option,
) *AsyncBlockRunner {
	ret := &AsyncBlockRunner{
		stopChannel:  make(chan chan struct{}),
		logger:       logger,
		db:           db,
		schedule:     schedule,
		maxBlockSize: maxBlockSize,
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

type AsyncBlockRunnerFactory struct{}

func (f *AsyncBlockRunnerFactory) CreateRunner(config AsyncBlockRunnerConfig) (any, error) {
	return func(
		logger logging.Logger,
		db *bun.DB,
		traceProvider trace.TracerProvider,
	) (worker.Runner, error) {
		parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		schedule, err := parser.Parse(config.HashLogsBlockCRONSpec)
		if err != nil {
			return nil, err
		}

		return NewAsyncBlockRunner(
			logger,
			db,
			schedule,
			config.HashLogsBlockMaxSize,
			WithTracer(traceProvider.Tracer("AsyncBlockRunner")),
		), nil
	}, nil
}

var _ worker.RunnerFactory[AsyncBlockRunnerConfig] = (*AsyncBlockRunnerFactory)(nil)

func init() {
	worker.RegisterRunnerFactory(&AsyncBlockRunnerFactory{})
}
