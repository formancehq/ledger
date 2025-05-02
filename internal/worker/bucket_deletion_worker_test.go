package worker

import (
	"context"
	"testing"
	"time"

	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"
)

type TestBucketDeletionWorker struct {
	BucketDeletionRunner
	mockDriver *driver.Driver
	mockTracer trace.Tracer
}

func TestBucketDeletionWorker_Name(t *testing.T) {
	t.Parallel()

	runner := &BucketDeletionRunner{}
	require.Equal(t, "Bucket deletion runner", runner.Name())
}

func TestBucketDeletionWorker_Run(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	worker := &TestBucketDeletionWorker{
		BucketDeletionRunner: BucketDeletionRunner{
			stopChannel: make(chan chan struct{}),
			logger:      NoOpLogger(),
			cfg: BucketDeletionRunnerConfig{
				Schedule:    mustParseCron("* * * * *"),
				GracePeriod: libtime.Duration(30 * 24 * time.Hour),
			},
			tracer: noop.Tracer{},
		},
		mockDriver: &driver.Driver{},
		mockTracer: noop.Tracer{},
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	err := worker.Stop(context.Background())
	require.NoError(t, err)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("Worker did not stop within timeout")
	}
}

func TestBucketDeletionWorker_Stop(t *testing.T) {
	t.Parallel()

	worker := &BucketDeletionRunner{
		stopChannel: make(chan chan struct{}),
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := worker.Stop(ctx)
	require.Error(t, err)
	require.Equal(t, context.Canceled, err)

	ctx, cancel = context.WithTimeout(context.Background(), 1*time.Millisecond)
	defer cancel()

	time.Sleep(5 * time.Millisecond)

	err = worker.Stop(ctx)
	require.Error(t, err)
	require.Equal(t, context.DeadlineExceeded, err)
}

func TestNewBucketDeletionRunner(t *testing.T) {
	t.Parallel()

	logger := NoOpLogger()
	driver := &driver.Driver{}
	schedule, err := cron.ParseStandard("* * * * *")
	require.NoError(t, err)

	cfg := BucketDeletionRunnerConfig{
		Schedule:    schedule,
		GracePeriod: libtime.Duration(30 * 24 * time.Hour),
	}

	runner := NewBucketDeletionRunner(logger, driver, cfg)
	require.NotNil(t, runner)
	require.Equal(t, logger, runner.logger)
	require.Equal(t, driver, runner.driver)
	require.Equal(t, cfg, runner.cfg)
	require.NotNil(t, runner.tracer)

	customTracer := noop.Tracer{}
	runner = NewBucketDeletionRunner(logger, driver, cfg, WithBucketDeletionTracer(customTracer))
	require.NotNil(t, runner)
	require.Equal(t, customTracer, runner.tracer)
}

func TestBucketDeletionRunner_RunWithError(t *testing.T) {
	t.Parallel()

	worker := &TestBucketDeletionWorker{
		BucketDeletionRunner: BucketDeletionRunner{
			stopChannel: make(chan chan struct{}),
			logger:      NoOpLogger(),
			cfg: BucketDeletionRunnerConfig{
				Schedule:    mustParseCron("* * * * *"),
				GracePeriod: libtime.Duration(30 * 24 * time.Hour),
			},
			tracer: noop.Tracer{},
		},
		mockDriver: &driver.Driver{},
		mockTracer: noop.Tracer{},
	}

	errorWorker := &TestBucketDeletionWorker{
		BucketDeletionRunner: worker.BucketDeletionRunner,
		mockDriver:           worker.mockDriver,
		mockTracer:           worker.mockTracer,
	}

	worker = errorWorker

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error)
	go func() {
		errCh <- worker.Run(ctx)
	}()

	time.Sleep(100 * time.Millisecond)

	err := worker.Stop(context.Background())
	require.NoError(t, err)

	select {
	case err := <-errCh:
		require.NoError(t, err)
	case <-time.After(1 * time.Second):
		t.Fatal("Worker did not stop within timeout")
	}
}
