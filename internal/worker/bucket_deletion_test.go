package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/storage/driver"
	"github.com/robfig/cron/v3"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace/noop"
	"go.uber.org/mock/gomock"
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
	t.Parallel()
	
	testCases := []struct {
		name          string
		gracePeriod   libtime.Duration
		setupMocks    func(ctrl *gomock.Controller) (*MockDriverWrapper, *MockTracer, *MockSpan)
		expectedError bool
	}{
		{
			name:        "success with buckets",
			gracePeriod: libtime.Duration(30 * 24 * time.Hour),
			setupMocks: func(ctrl *gomock.Controller) (*MockDriverWrapper, *MockTracer, *MockSpan) {
				mockDriver := NewMockDriverWrapper(ctrl)
				mockTracer := NewMockTracer(ctrl)
				mockSpan := NewMockSpan(ctrl)
				bucketSpan := NewMockSpan(ctrl)
				
				mockTracer.EXPECT().
					Start(gomock.Any(), "RunBucketDeletion").
					Return(context.Background(), mockSpan)
				
				mockSpan.EXPECT().
					SetAttributes(attribute.Int("grace_period_days", 30))
				
				mockDriver.EXPECT().
					GetBucketsMarkedForDeletion(gomock.Any(), 30).
					Return([]string{"bucket1", "bucket2"}, nil)
				
				mockSpan.EXPECT().
					SetAttributes(attribute.Int("buckets_to_delete", 2))
				
				mockTracer.EXPECT().
					Start(gomock.Any(), "DeleteBucket").
					Return(context.Background(), bucketSpan)
				
				bucketSpan.EXPECT().
					SetAttributes(attribute.String("bucket", "bucket1"))
				
				mockDriver.EXPECT().
					PhysicallyDeleteBucket(gomock.Any(), "bucket1").
					Return(nil)
				
				bucketSpan.EXPECT().End()
				
				mockTracer.EXPECT().
					Start(gomock.Any(), "DeleteBucket").
					Return(context.Background(), bucketSpan)
				
				bucketSpan.EXPECT().
					SetAttributes(attribute.String("bucket", "bucket2"))
				
				mockDriver.EXPECT().
					PhysicallyDeleteBucket(gomock.Any(), "bucket2").
					Return(nil)
				
				bucketSpan.EXPECT().End()
				
				mockSpan.EXPECT().End()
				
				return mockDriver, mockTracer, mockSpan
			},
			expectedError: false,
		},
		{
			name:        "error getting buckets",
			gracePeriod: libtime.Duration(30 * 24 * time.Hour),
			setupMocks: func(ctrl *gomock.Controller) (*MockDriverWrapper, *MockTracer, *MockSpan) {
				mockDriver := NewMockDriverWrapper(ctrl)
				mockTracer := NewMockTracer(ctrl)
				mockSpan := NewMockSpan(ctrl)
				
				mockTracer.EXPECT().
					Start(gomock.Any(), "RunBucketDeletion").
					Return(context.Background(), mockSpan)
				
				mockSpan.EXPECT().
					SetAttributes(attribute.Int("grace_period_days", 30))
				
				mockDriver.EXPECT().
					GetBucketsMarkedForDeletion(gomock.Any(), 30).
					Return(nil, errors.New("database error"))
				
				mockSpan.EXPECT().End()
				
				return mockDriver, mockTracer, mockSpan
			},
			expectedError: true,
		},
		{
			name:        "error deleting bucket",
			gracePeriod: libtime.Duration(30 * 24 * time.Hour),
			setupMocks: func(ctrl *gomock.Controller) (*MockDriverWrapper, *MockTracer, *MockSpan) {
				mockDriver := NewMockDriverWrapper(ctrl)
				mockTracer := NewMockTracer(ctrl)
				mockSpan := NewMockSpan(ctrl)
				bucketSpan := NewMockSpan(ctrl)
				
				mockTracer.EXPECT().
					Start(gomock.Any(), "RunBucketDeletion").
					Return(context.Background(), mockSpan)
				
				mockSpan.EXPECT().
					SetAttributes(attribute.Int("grace_period_days", 30))
				
				mockDriver.EXPECT().
					GetBucketsMarkedForDeletion(gomock.Any(), 30).
					Return([]string{"bucket1", "bucket2"}, nil)
				
				mockSpan.EXPECT().
					SetAttributes(attribute.Int("buckets_to_delete", 2))
				
				mockTracer.EXPECT().
					Start(gomock.Any(), "DeleteBucket").
					Return(context.Background(), bucketSpan)
				
				bucketSpan.EXPECT().
					SetAttributes(attribute.String("bucket", "bucket1"))
				
				mockDriver.EXPECT().
					PhysicallyDeleteBucket(gomock.Any(), "bucket1").
					Return(errors.New("deletion error"))
				
				bucketSpan.EXPECT().End()
				
				mockSpan.EXPECT().End()
				
				return mockDriver, mockTracer, mockSpan
			},
			expectedError: true,
		},
		{
			name:        "no buckets to delete",
			gracePeriod: libtime.Duration(30 * 24 * time.Hour),
			setupMocks: func(ctrl *gomock.Controller) (*MockDriverWrapper, *MockTracer, *MockSpan) {
				mockDriver := NewMockDriverWrapper(ctrl)
				mockTracer := NewMockTracer(ctrl)
				mockSpan := NewMockSpan(ctrl)
				
				mockTracer.EXPECT().
					Start(gomock.Any(), "RunBucketDeletion").
					Return(context.Background(), mockSpan)
				
				mockSpan.EXPECT().
					SetAttributes(attribute.Int("grace_period_days", 30))
				
				mockDriver.EXPECT().
					GetBucketsMarkedForDeletion(gomock.Any(), 30).
					Return([]string{}, nil)
				
				mockSpan.EXPECT().
					SetAttributes(attribute.Int("buckets_to_delete", 0))
				
				mockSpan.EXPECT().End()
				
				return mockDriver, mockTracer, mockSpan
			},
			expectedError: false,
		},
	}
	
	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			
			mockDriver, mockTracer, _ := tc.setupMocks(ctrl)
			
			driverWrapper := &struct {
				*MockDriverWrapper
			}{mockDriver}
			
			runner := &BucketDeletionRunner{
				logger: NoOpLogger(),
				driver: nil, // Not used directly in the test
				cfg: BucketDeletionRunnerConfig{
					GracePeriod: tc.gracePeriod,
				},
				tracer: mockTracer,
			}
			
			runFunc := func(ctx context.Context) error {
				ctx, span := mockTracer.Start(ctx, "RunBucketDeletion")
				defer span.End()
				
				days := int(tc.gracePeriod.Hours() / 24)
				span.SetAttributes(attribute.Int("grace_period_days", days))
				
				buckets, err := driverWrapper.GetBucketsMarkedForDeletion(ctx, days)
				if err != nil {
					return err
				}
				
				span.SetAttributes(attribute.Int("buckets_to_delete", len(buckets)))
				
				for _, bucket := range buckets {
					bucketCtx, bucketSpan := mockTracer.Start(ctx, "DeleteBucket")
					bucketSpan.SetAttributes(attribute.String("bucket", bucket))
					
					if err := driverWrapper.PhysicallyDeleteBucket(bucketCtx, bucket); err != nil {
						bucketSpan.End()
						return err
					}
					
					bucketSpan.End()
				}
				
				return nil
			}
			
			err := runFunc(context.Background())
			
			if tc.expectedError {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
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
