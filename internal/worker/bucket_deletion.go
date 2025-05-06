package worker

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/robfig/cron/v3"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
)

type BucketDeletionRunnerConfig struct {
	Schedule     cron.Schedule
	GracePeriod  time.Duration // Duration after which buckets marked for deletion will be physically deleted
}

type BucketDeletionRunner struct {
	stopChannel chan chan struct{}
	logger      logging.Logger
	driver      *driver.Driver
	cfg         BucketDeletionRunnerConfig
	tracer      trace.Tracer
}

func (r *BucketDeletionRunner) Name() string {
	return "Bucket deletion runner"
}

func (r *BucketDeletionRunner) Run(ctx context.Context) error {
	now := time.Now()
	next := r.cfg.Schedule.Next(now.Time).Sub(now.Time)

	for {
		select {
		case <-time.After(next):
			if err := r.run(ctx); err != nil {
				r.logger.Errorf("error running bucket deletion: %v", err)
			}

			now = time.Now()
			next = r.cfg.Schedule.Next(now.Time).Sub(now.Time)
		case ch := <-r.stopChannel:
			close(ch)
			return nil
		}
	}
}

func (r *BucketDeletionRunner) Stop(ctx context.Context) error {
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

func (r *BucketDeletionRunner) run(ctx context.Context) error {
	ctx, span := r.tracer.Start(ctx, "RunBucketDeletion")
	defer span.End()

	gracePeriodHours := r.cfg.GracePeriod.Hours()
	span.SetAttributes(attribute.Float64("grace_period_hours", gracePeriodHours))

	buckets, err := r.driver.GetBucketsMarkedForDeletion(ctx, r.cfg.GracePeriod)
	if err != nil {
		return fmt.Errorf("getting buckets marked for deletion: %w", err)
	}

	span.SetAttributes(attribute.Int("buckets_to_delete", len(buckets)))
	r.logger.Infof("Found %d buckets to physically delete", len(buckets))

	for _, bucket := range buckets {
		bucketCtx, bucketSpan := r.tracer.Start(ctx, "DeleteBucket")
		bucketSpan.SetAttributes(attribute.String("bucket", bucket))

		r.logger.Infof("Physically deleting bucket: %s", bucket)
		if err := r.driver.PhysicallyDeleteBucket(bucketCtx, bucket); err != nil {
			bucketSpan.End()
			return fmt.Errorf("physically deleting bucket %s: %w", bucket, err)
		}

		bucketSpan.End()
		r.logger.Infof("Successfully deleted bucket: %s", bucket)
	}

	return nil
}

func NewBucketDeletionRunner(logger logging.Logger, driver *driver.Driver, cfg BucketDeletionRunnerConfig, opts ...BucketDeletionOption) *BucketDeletionRunner {
	ret := &BucketDeletionRunner{
		stopChannel: make(chan chan struct{}),
		logger:      logger,
		driver:      driver,
		cfg:         cfg,
	}

	for _, opt := range append(defaultBucketDeletionOptions, opts...) {
		opt(ret)
	}

	return ret
}

type BucketDeletionOption func(*BucketDeletionRunner)

func WithBucketDeletionTracer(tracer trace.Tracer) BucketDeletionOption {
	return func(r *BucketDeletionRunner) {
		r.tracer = tracer
	}
}

var defaultBucketDeletionOptions = []BucketDeletionOption{
	WithBucketDeletionTracer(noop.Tracer{}),
}
