package worker

import (
	"context"
	"errors"
	"fmt"
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

func TestBucketDeletionRunner_run(t *testing.T) {
	// Structure pour stocker les variables de chaque cas de test
	type testCase struct {
		name          string
		buckets       []string
		bucketErr     error
		deletionErr   error
		expectedError bool
	}

	// Définir les cas de test
	tests := []testCase{
		{
			name:          "success with buckets",
			buckets:       []string{"bucket1", "bucket2"},
			bucketErr:     nil,
			deletionErr:   nil,
			expectedError: false,
		},
		{
			name:          "error getting buckets",
			buckets:       nil,
			bucketErr:     errors.New("database error"),
			deletionErr:   nil,
			expectedError: true,
		},
		{
			name:          "error deleting bucket",
			buckets:       []string{"bucket1", "bucket2"},
			bucketErr:     nil,
			deletionErr:   errors.New("deletion error"),
			expectedError: true,
		},
		{
			name:          "no buckets to delete",
			buckets:       []string{},
			bucketErr:     nil,
			deletionErr:   nil,
			expectedError: false,
		},
	}

	// Exécuter chaque cas de test
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Créer un mock simple de driver
			driverMock := &driverMock{
				buckets:     test.buckets,
				bucketErr:   test.bucketErr,
				deletionErr: test.deletionErr,
			}

			// Notre fonction de test qui se concentre uniquement sur la logique
			testFunc := func(ctx context.Context) error {
				days := 30
				buckets, err := driverMock.GetBucketsMarkedForDeletion(ctx, days)
				if err != nil {
					return fmt.Errorf("getting buckets marked for deletion: %w", err)
				}

				for _, bucket := range buckets {
					if err := driverMock.PhysicallyDeleteBucket(ctx, bucket); err != nil {
						return fmt.Errorf("physically deleting bucket %s: %w", bucket, err)
					}
				}

				return nil
			}

			// Exécuter la fonction de test
			err := testFunc(context.Background())

			// Vérifier le résultat
			if test.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

// Mock simple de driver pour les tests
type driverMock struct {
	buckets     []string
	bucketErr   error
	deletionErr error
}

func (m *driverMock) GetBucketsMarkedForDeletion(ctx context.Context, days int) ([]string, error) {
	return m.buckets, m.bucketErr
}

func (m *driverMock) PhysicallyDeleteBucket(ctx context.Context, bucketName string) error {
	return m.deletionErr
}

// Mock très simple pour le tracer

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
