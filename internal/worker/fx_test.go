package worker

import (
	"testing"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestNewFXModule(t *testing.T) {
	t.Parallel()

	cfg := ModuleConfig{
		Schedule:                  "* * * * *",
		MaxBlockSize:              1000,
		BucketDeletionSchedule:    "0 0 0 * * *",
		BucketDeletionGracePeriod: "30d",
	}

	module := NewFXModule(cfg)
	require.NotNil(t, module)
}

func TestBucketDeletionProviders(t *testing.T) {
	t.Parallel()

	logger := NoOpLogger()
	mockDriver := &driver.Driver{}
	tracerProvider := noop.NewTracerProvider()

	cfg := ModuleConfig{
		BucketDeletionSchedule:    "",
		BucketDeletionGracePeriod: "0h",
	}

	providerFunc := func() (*BucketDeletionRunner, error) {
		bucketDeletionSchedule := cfg.BucketDeletionSchedule
		if bucketDeletionSchedule == "" {
			bucketDeletionSchedule = "0 0 0 * * *" // Daily at midnight
		}

		gracePeriod := cfg.BucketDeletionGracePeriod
		if gracePeriod == "" {
			gracePeriod = "720h" // Default 30 days
		}

		parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		schedule, err := parser.Parse(bucketDeletionSchedule)
		if err != nil {
			return nil, err
		}

		duration, err := time.ParseDuration(gracePeriod)
		if err != nil {
			return nil, err
		}

		return NewBucketDeletionRunner(logger, mockDriver, BucketDeletionRunnerConfig{
			Schedule:    schedule,
			GracePeriod: duration,
		}, WithBucketDeletionTracer(tracerProvider.Tracer("BucketDeletionRunner"))), nil
	}

	runner, err := providerFunc()
	require.NoError(t, err)
	require.NotNil(t, runner)
	require.Equal(t, mockDriver, runner.driver)
	require.Equal(t, logger, runner.logger)
	require.Equal(t, 30*24*time.Hour, runner.cfg.GracePeriod)

	cfg = ModuleConfig{
		BucketDeletionSchedule:    "*/5 * * * * *",
		BucketDeletionGracePeriod: "45d",
	}

	runner, err = providerFunc()
	require.NoError(t, err)
	require.NotNil(t, runner)
	require.Equal(t, 45*24*time.Hour, runner.cfg.GracePeriod)
}

func TestAsyncBlockProviders(t *testing.T) {
	t.Parallel()

	logger := NoOpLogger()
	db := &bun.DB{}
	tracerProvider := noop.NewTracerProvider()

	cfg := ModuleConfig{
		Schedule:     "* * * * * *",
		MaxBlockSize: 1000,
	}

	providerFunc := func() (*AsyncBlockRunner, error) {
		parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		schedule, err := parser.Parse(cfg.Schedule)
		if err != nil {
			return nil, err
		}

		return NewAsyncBlockRunner(logger, db, AsyncBlockRunnerConfig{
			MaxBlockSize: cfg.MaxBlockSize,
			Schedule:     schedule,
		}, WithTracer(tracerProvider.Tracer("AsyncBlockRunner"))), nil
	}

	runner, err := providerFunc()
	require.NoError(t, err)
	require.NotNil(t, runner)
	require.Equal(t, db, runner.db)
	require.Equal(t, logger, runner.logger)
	require.Equal(t, 1000, runner.cfg.MaxBlockSize)
}
