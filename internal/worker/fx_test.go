package worker

import (
	"testing"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/fx"
)

func TestNewFXModule(t *testing.T) {
	t.Parallel()

	cfg := ModuleConfig{
		Schedule:                "* * * * *",
		MaxBlockSize:            1000,
		BucketDeletionSchedule:  "0 0 0 * * *",
		BucketDeletionGraceDays: 30,
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
		BucketDeletionSchedule:  "",
		BucketDeletionGraceDays: 0,
	}

	providerFunc := func() (*BucketDeletionRunner, error) {
		bucketDeletionSchedule := cfg.BucketDeletionSchedule
		if bucketDeletionSchedule == "" {
			bucketDeletionSchedule = "0 0 0 * * *" // Daily at midnight
		}
		
		graceDays := cfg.BucketDeletionGraceDays
		if graceDays <= 0 {
			graceDays = 30 // Default 30 days
		}
		
		parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)
		schedule, err := parser.Parse(bucketDeletionSchedule)
		if err != nil {
			return nil, err
		}

		return NewBucketDeletionRunner(logger, mockDriver, BucketDeletionRunnerConfig{
			Schedule:    schedule,
			GracePeriod: time.Duration(graceDays) * 24 * time.Hour,
		}, WithBucketDeletionTracer(tracerProvider.Tracer("BucketDeletionRunner"))), nil
	}

	runner, err := providerFunc()
	require.NoError(t, err)
	require.NotNil(t, runner)
	require.Equal(t, mockDriver, runner.driver)
	require.Equal(t, logger, runner.logger)
	require.Equal(t, time.Duration(30*24)*time.Hour, runner.cfg.GracePeriod)

	cfg = ModuleConfig{
		BucketDeletionSchedule:  "*/5 * * * *",
		BucketDeletionGraceDays: 45,
	}

	runner, err = providerFunc()
	require.NoError(t, err)
	require.NotNil(t, runner)
	require.Equal(t, time.Duration(45*24)*time.Hour, runner.cfg.GracePeriod)
}

func TestAsyncBlockProviders(t *testing.T) {
	t.Parallel()

	logger := NoOpLogger()
	db := &bun.DB{}
	tracerProvider := noop.NewTracerProvider()

	cfg := ModuleConfig{
		Schedule:     "* * * * *",
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
