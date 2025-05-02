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
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/mock/gomock"
)

type TestDriverWrapper struct {
	mock *MockDriverWrapper
}

func (d *TestDriverWrapper) GetBucketsMarkedForDeletion(ctx context.Context, days int) ([]string, error) {
	return d.mock.GetBucketsMarkedForDeletion(ctx, days)
}

func (d *TestDriverWrapper) PhysicallyDeleteBucket(ctx context.Context, bucketName string) error {
	return d.mock.PhysicallyDeleteBucket(ctx, bucketName)
}

func TestBucketDeletionRunner_Name(t *testing.T) {
	t.Parallel()
	
	runner := &BucketDeletionRunner{}
	require.Equal(t, "Bucket deletion runner", runner.Name())
}

func TestBucketDeletionRunner_run(t *testing.T) {
	t.Parallel()
	
	testCases := []struct {
		name           string
		gracePeriod    libtime.Duration
		setupMock      func(driver *MockDriverWrapper, tracer *MockTracer, span *MockSpan)
		expectedError  bool
	}{
		{
			name:        "success with no buckets",
			gracePeriod: libtime.Duration(30 * 24 * time.Hour),
			setupMock: func(d *MockDriverWrapper, tracer *MockTracer, span *MockSpan) {
				tracer.EXPECT().
					Start(gomock.Any(), "RunBucketDeletion", gomock.Any()).
					Return(context.Background(), span)
				
				span.EXPECT().
					SetAttributes(attribute.Int("grace_period_days", 30))
				
				d.EXPECT().
					GetBucketsMarkedForDeletion(gomock.Any(), 30).
					Return([]string{}, nil)
				
				span.EXPECT().
					SetAttributes(attribute.Int("buckets_to_delete", 0))
				
				span.EXPECT().End()
			},
			expectedError: false,
		},
		{
			name:        "success with buckets",
			gracePeriod: libtime.Duration(30 * 24 * time.Hour),
			setupMock: func(d *MockDriverWrapper, tracer *MockTracer, span *MockSpan) {
				bucketSpan := NewMockSpan(gomock.NewController(t))
				
				tracer.EXPECT().
					Start(gomock.Any(), "RunBucketDeletion", gomock.Any()).
					Return(context.Background(), span)
				
				span.EXPECT().
					SetAttributes(attribute.Int("grace_period_days", 30))
				
				d.EXPECT().
					GetBucketsMarkedForDeletion(gomock.Any(), 30).
					Return([]string{"bucket1", "bucket2"}, nil)
				
				span.EXPECT().
					SetAttributes(attribute.Int("buckets_to_delete", 2))
				
				tracer.EXPECT().
					Start(gomock.Any(), "DeleteBucket", gomock.Any()).
					Return(context.Background(), bucketSpan)
				
				bucketSpan.EXPECT().
					SetAttributes(attribute.String("bucket", "bucket1"))
				
				d.EXPECT().
					PhysicallyDeleteBucket(gomock.Any(), "bucket1").
					Return(nil)
				
				bucketSpan.EXPECT().End()
				
				tracer.EXPECT().
					Start(gomock.Any(), "DeleteBucket", gomock.Any()).
					Return(context.Background(), bucketSpan)
				
				bucketSpan.EXPECT().
					SetAttributes(attribute.String("bucket", "bucket2"))
				
				d.EXPECT().
					PhysicallyDeleteBucket(gomock.Any(), "bucket2").
					Return(nil)
				
				bucketSpan.EXPECT().End()
				
				span.EXPECT().End()
			},
			expectedError: false,
		},
		{
			name:        "error getting buckets",
			gracePeriod: libtime.Duration(30 * 24 * time.Hour),
			setupMock: func(d *MockDriverWrapper, tracer *MockTracer, span *MockSpan) {
				tracer.EXPECT().
					Start(gomock.Any(), "RunBucketDeletion", gomock.Any()).
					Return(context.Background(), span)
				
				span.EXPECT().
					SetAttributes(attribute.Int("grace_period_days", 30))
				
				d.EXPECT().
					GetBucketsMarkedForDeletion(gomock.Any(), 30).
					Return(nil, errors.New("database error"))
				
				span.EXPECT().End()
			},
			expectedError: true,
		},
		{
			name:        "error deleting bucket",
			gracePeriod: libtime.Duration(30 * 24 * time.Hour),
			setupMock: func(d *MockDriverWrapper, tracer *MockTracer, span *MockSpan) {
				bucketSpan := NewMockSpan(gomock.NewController(t))
				
				tracer.EXPECT().
					Start(gomock.Any(), "RunBucketDeletion", gomock.Any()).
					Return(context.Background(), span)
				
				span.EXPECT().
					SetAttributes(attribute.Int("grace_period_days", 30))
				
				d.EXPECT().
					GetBucketsMarkedForDeletion(gomock.Any(), 30).
					Return([]string{"bucket1", "bucket2"}, nil)
				
				span.EXPECT().
					SetAttributes(attribute.Int("buckets_to_delete", 2))
				
				tracer.EXPECT().
					Start(gomock.Any(), "DeleteBucket", gomock.Any()).
					Return(context.Background(), bucketSpan)
				
				bucketSpan.EXPECT().
					SetAttributes(attribute.String("bucket", "bucket1"))
				
				d.EXPECT().
					PhysicallyDeleteBucket(gomock.Any(), "bucket1").
					Return(errors.New("deletion error"))
				
				bucketSpan.EXPECT().End()
				
				span.EXPECT().End()
			},
			expectedError: true,
		},
	}
	
	for _, tc := range testCases {
		tc := tc // capture range variable
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()
			
			mockDriver := NewMockDriverWrapper(ctrl)
			noopTracer := trace.NewNoopTracerProvider().Tracer("test")
			mockSpan := NewMockSpan(ctrl)
			
			tc.setupMock(mockDriver, NewMockTracer(ctrl), mockSpan)
			
			runner := &BucketDeletionRunner{
				logger: NoOpLogger(),
				driver: &driver.Driver{}, // Placeholder, we'll use mockDriver for actual calls
				cfg: BucketDeletionRunnerConfig{
					GracePeriod: tc.gracePeriod,
				},
				tracer: noopTracer,
			}
			
			
			err := runner.run(context.Background())
			
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
	
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	
	schedule, err := cron.ParseStandard("* * * * *")
	require.NoError(t, err)
	
	driverAdapter := &driver.Driver{}
	
	runner := NewBucketDeletionRunner(
		NoOpLogger(),
		driverAdapter, // Use the real driver type
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
