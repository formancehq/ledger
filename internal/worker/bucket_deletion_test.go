package worker

import (
	"context"
	"testing"
	"time"

	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace/noop"
)

func TestBucketDeletionRunner_Name(t *testing.T) {
	t.Parallel()
	
	runner := &BucketDeletionRunner{}
	require.Equal(t, "Bucket deletion runner", runner.Name())
}

func TestBucketDeletionRunner_Config(t *testing.T) {
	t.Parallel()
	
	gracePeriod := libtime.Duration(30 * 24 * time.Hour)
	schedule, err := cron.ParseStandard("0 0 * * *") // Daily at midnight
	require.NoError(t, err)
	
	runner := &BucketDeletionRunner{
		logger: NoOpLogger(),
		driver: &driver.Driver{},
		cfg: BucketDeletionRunnerConfig{
			Schedule:    schedule,
			GracePeriod: gracePeriod,
		},
		tracer: noop.Tracer{},
	}
	
	require.Equal(t, gracePeriod, runner.cfg.GracePeriod)
	require.Equal(t, schedule, runner.cfg.Schedule)
}

func TestNewBucketDeletionRunner(t *testing.T) {
	t.Parallel()
	
	logger := NoOpLogger()
	driverAdapter := &driver.Driver{}
	schedule, err := cron.ParseStandard("* * * * *")
	require.NoError(t, err)
	
	cfg := BucketDeletionRunnerConfig{
		Schedule:    schedule,
		GracePeriod: libtime.Duration(30 * 24 * time.Hour),
	}
	
	runner := NewBucketDeletionRunner(logger, driverAdapter, cfg)
	require.NotNil(t, runner)
	require.Equal(t, logger, runner.logger)
	require.Equal(t, driverAdapter, runner.driver)
	require.Equal(t, cfg, runner.cfg)
	require.NotNil(t, runner.tracer)
	
	customTracer := noop.Tracer{}
	runner = NewBucketDeletionRunner(logger, driverAdapter, cfg, WithBucketDeletionTracer(customTracer))
	require.NotNil(t, runner)
	require.Equal(t, customTracer, runner.tracer)
}

func TestBucketDeletionRunner_RunAndStop(t *testing.T) {
	t.Parallel()
	
	schedule, err := cron.ParseStandard("* * * * *")
	require.NoError(t, err)
	
	driverAdapter := &driver.Driver{}
	
	runner := NewBucketDeletionRunner(
		NoOpLogger(),
		driverAdapter,
		BucketDeletionRunnerConfig{
			Schedule:    schedule,
			GracePeriod: libtime.Duration(30 * 24 * time.Hour),
		},
	)
	
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	
	errCh := make(chan error)
	go func() {
		errCh <- runner.Run(ctx)
	}()
	
	time.Sleep(100 * time.Millisecond)
	
	err = runner.Stop(context.Background())
	require.NoError(t, err)
	
	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("Runner did not stop within timeout")
	}
}

func TestBucketDeletionRunner_StopWithCanceledContext(t *testing.T) {
	t.Parallel()
	
	runner := &BucketDeletionRunner{
		stopChannel: make(chan chan struct{}),
	}
	
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	
	err := runner.Stop(ctx)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)
}

func TestBucketDeletionRunner_StopWithDeadlineExceeded(t *testing.T) {
	t.Parallel()
	
	runner := &BucketDeletionRunner{
		stopChannel: make(chan chan struct{}),
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()
	
	time.Sleep(5 * time.Millisecond)
	
	err := runner.Stop(ctx)
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
}
