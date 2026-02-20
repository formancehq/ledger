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

	"github.com/formancehq/go-libs/v4/logging"

	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

type BucketCleanupRunnerConfig struct {
	RetentionPeriod time.Duration
	Schedule        cron.Schedule
}

type BucketCleanupRunner struct {
	stopChannel chan chan struct{}
	logger      logging.Logger
	db          *bun.DB
	cfg         BucketCleanupRunnerConfig
	tracer      trace.Tracer
}

func (r *BucketCleanupRunner) Name() string {
	return "Bucket cleanup runner"
}

func (r *BucketCleanupRunner) Run(ctx context.Context) error {
	now := time.Now()
	next := r.cfg.Schedule.Next(now).Sub(now)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx); err != nil {
				r.logger.Errorf("error running bucket cleanup: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now).Sub(now)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *BucketCleanupRunner) Stop(ctx context.Context) error {
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

func (r *BucketCleanupRunner) run(ctx context.Context) error {
	ctx, span := r.tracer.Start(ctx, "Run")
	defer span.End()

	// Calculate the cutoff time: buckets deleted before this time should be hard deleted
	cutoffTime := time.Now().Add(-r.cfg.RetentionPeriod)
	span.SetAttributes(attribute.String("cutoff_time", cutoffTime.Format(time.RFC3339)))

	systemStore := systemstore.New(r.db)
	buckets, err := systemStore.GetDeletedBucketsOlderThan(ctx, cutoffTime)
	if err != nil {
		return fmt.Errorf("getting deleted buckets: %w", err)
	}

	span.SetAttributes(attribute.Int("buckets_to_delete", len(buckets)))

	var failedBuckets []string
	successCount := 0

	for _, bucket := range buckets {
		if err := r.processBucket(ctx, bucket); err != nil {
			r.logger.Errorf("error processing bucket %s: %v", bucket, err)
			failedBuckets = append(failedBuckets, bucket)
			// Continue with other buckets even if one fails
			continue
		}
	}

	span.SetAttributes(
		attribute.Int("buckets_deleted", successCount),
		attribute.StringSlice("buckets_failed", failedBuckets),
	)

	if len(failedBuckets) > 0 {
		r.logger.Errorf("bucket cleanup completed with %d failures: %v", len(failedBuckets), failedBuckets)
	}

	return nil
}

func (r *BucketCleanupRunner) processBucket(ctx context.Context, bucket string) error {
	ctx, span := r.tracer.Start(ctx, "ProcessBucket")
	defer span.End()

	span.SetAttributes(attribute.String("bucket", bucket))

	systemStore := systemstore.New(r.db)
	if err := systemStore.HardDeleteBucket(ctx, bucket); err != nil {
		return fmt.Errorf("hard deleting bucket %s: %w", bucket, err)
	}

	r.logger.Infof("Successfully hard deleted bucket: %s", bucket)
	return nil
}

// NewBucketCleanupRunner creates a BucketCleanupRunner configured with the provided logger,
// database handle, and configuration, applying any functional options.
//
// The returned runner is ready to be started; provided options override default behavior.
func NewBucketCleanupRunner(logger logging.Logger, db *bun.DB, cfg BucketCleanupRunnerConfig, opts ...BucketCleanupRunnerOption) *BucketCleanupRunner {
	ret := &BucketCleanupRunner{
		stopChannel: make(chan chan struct{}),
		logger:      logger,
		db:          db,
		cfg:         cfg,
	}

	for _, opt := range append(defaultBucketCleanupRunnerOptions, opts...) {
		opt(ret)
	}

	return ret
}

type BucketCleanupRunnerOption func(*BucketCleanupRunner)

// WithBucketCleanupRunnerTracer returns a BucketCleanupRunnerOption that sets the OpenTelemetry tracer used by the BucketCleanupRunner.
func WithBucketCleanupRunnerTracer(tracer trace.Tracer) BucketCleanupRunnerOption {
	return func(r *BucketCleanupRunner) {
		r.tracer = tracer
	}
}

var defaultBucketCleanupRunnerOptions = []BucketCleanupRunnerOption{
	WithBucketCleanupRunnerTracer(noop.Tracer{}),
}

// NewBucketCleanupRunnerModule returns an Fx module that provides a configured BucketCleanupRunner
// and registers lifecycle hooks to start it in the background when the application starts and to stop
// it when the application shuts down. The background goroutine will panic if the runner's Run method
// returns an error.
func NewBucketCleanupRunnerModule(cfg BucketCleanupRunnerConfig) fx.Option {
	return fx.Options(
		fx.Provide(func(logger logging.Logger, db *bun.DB) (*BucketCleanupRunner, error) {
			return NewBucketCleanupRunner(logger, db, cfg), nil
		}),
		fx.Invoke(func(lc fx.Lifecycle, bucketCleanupRunner *BucketCleanupRunner) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						if err := bucketCleanupRunner.Run(context.WithoutCancel(ctx)); err != nil {
							panic(err)
						}
					}()

					return nil
				},
				OnStop: bucketCleanupRunner.Stop,
			})
		}),
	)
}
