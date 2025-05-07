package worker

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/robfig/cron/v3"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
)

type ModuleConfig struct {
	Schedule                  string
	MaxBlockSize              int
	BucketDeletionSchedule    string
	BucketDeletionGracePeriod string
}

func NewFXModule(cfg ModuleConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(
			logger logging.Logger,
			db *bun.DB,
			traceProvider trace.TracerProvider,
		) (*AsyncBlockRunner, error) {
			parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
			schedule, err := parser.Parse(cfg.Schedule)
			if err != nil {
				return nil, err
			}

			return NewAsyncBlockRunner(logger, db, AsyncBlockRunnerConfig{
				MaxBlockSize: cfg.MaxBlockSize,
				Schedule:     schedule,
			}, WithTracer(traceProvider.Tracer("AsyncBlockRunner"))), nil
		}),
		fx.Provide(func(
			logger logging.Logger,
			driver *driver.Driver,
			traceProvider trace.TracerProvider,
		) (*BucketDeletionRunner, error) {
			bucketDeletionSchedule := cfg.BucketDeletionSchedule
			if bucketDeletionSchedule == "" {
				bucketDeletionSchedule = "0 0 0 * * *" // Daily at midnight
			}

			gracePeriod := cfg.BucketDeletionGracePeriod
			if gracePeriod == "" {
				gracePeriod = "720h" // Default 30 days (720 hours)
			}

			parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
			schedule, err := parser.Parse(bucketDeletionSchedule)
			if err != nil {
				return nil, err
			}

			duration, err := time.ParseDuration(gracePeriod)
			if err != nil {
				return nil, fmt.Errorf("parsing grace period duration: %w", err)
			}

			return NewBucketDeletionRunner(logger, driver, BucketDeletionRunnerConfig{
				Schedule:    schedule,
				GracePeriod: duration,
			}, WithBucketDeletionTracer(traceProvider.Tracer("BucketDeletionRunner"))), nil
		}),
		fx.Invoke(fx.Annotate(func(lc fx.Lifecycle, asyncBlockRunner *AsyncBlockRunner) {
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
		}, fx.ParamTags(``, ``))),
		fx.Invoke(fx.Annotate(func(lc fx.Lifecycle, bucketDeletionRunner *BucketDeletionRunner) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := bucketDeletionRunner.Run(context.WithoutCancel(ctx)); err != nil {
							panic(err)
						}
					}()

					return nil
				},
				OnStop: bucketDeletionRunner.Stop,
			})
		}, fx.ParamTags(``, ``))),
	)
}
