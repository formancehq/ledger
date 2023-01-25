package task

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/google/uuid"

	"go.uber.org/dig"

	"github.com/formancehq/payments/internal/app/models"

	"github.com/formancehq/go-libs/logging/logginglogrus"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
)

//nolint:gochecknoglobals // allow in tests
var DefaultContainerFactory = ContainerCreateFunc(func(ctx context.Context, descriptor models.TaskDescriptor, taskID uuid.UUID) (*dig.Container, error) {
	return dig.New(), nil
})

func newDescriptor() models.TaskDescriptor {
	return []byte(uuid.New().String())
}

func TaskTerminatedWithStatus(store *InMemoryStore, provider models.ConnectorProvider,
	descriptor models.TaskDescriptor, expectedStatus models.TaskStatus, errString string,
) func() bool {
	return func() bool {
		status, resultErr, ok := store.Result(provider, descriptor)
		if !ok {
			return false
		}

		if resultErr != errString {
			return false
		}

		return status == expectedStatus
	}
}

func TaskTerminated(store *InMemoryStore, provider models.ConnectorProvider, descriptor models.TaskDescriptor) func() bool {
	return TaskTerminatedWithStatus(store, provider, descriptor, models.TaskStatusTerminated, "")
}

func TaskFailed(store *InMemoryStore, provider models.ConnectorProvider, descriptor models.TaskDescriptor, errStr string) func() bool {
	return TaskTerminatedWithStatus(store, provider, descriptor, models.TaskStatusFailed, errStr)
}

func TaskPending(store *InMemoryStore, provider models.ConnectorProvider, descriptor models.TaskDescriptor) func() bool {
	return TaskTerminatedWithStatus(store, provider, descriptor, models.TaskStatusPending, "")
}

func TaskActive(store *InMemoryStore, provider models.ConnectorProvider, descriptor models.TaskDescriptor) func() bool {
	return TaskTerminatedWithStatus(store, provider, descriptor, models.TaskStatusActive, "")
}

func TestTaskScheduler(t *testing.T) {
	t.Parallel()

	l := logrus.New()
	if testing.Verbose() {
		l.SetLevel(logrus.DebugLevel)
	}

	logger := logginglogrus.New(l)

	t.Run("Nominal", func(t *testing.T) {
		t.Parallel()

		store := NewInMemoryStore()
		provider := models.ConnectorProvider(uuid.New().String())
		done := make(chan struct{})

		scheduler := NewDefaultScheduler(provider, logger, store,
			DefaultContainerFactory, ResolverFn(func(descriptor models.TaskDescriptor) Task {
				return func(ctx context.Context) error {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-done:
						return nil
					}
				}
			}), 1)

		descriptor := newDescriptor()
		err := scheduler.Schedule(descriptor, false)
		require.NoError(t, err)

		require.Eventually(t, TaskActive(store, provider, descriptor), time.Second, 100*time.Millisecond)
		close(done)
		require.Eventually(t, TaskTerminated(store, provider, descriptor), time.Second, 100*time.Millisecond)
	})

	t.Run("Duplicate task", func(t *testing.T) {
		t.Parallel()

		store := NewInMemoryStore()
		provider := models.ConnectorProvider(uuid.New().String())
		scheduler := NewDefaultScheduler(provider, logger, store, DefaultContainerFactory,
			ResolverFn(func(descriptor models.TaskDescriptor) Task {
				return func(ctx context.Context) error {
					<-ctx.Done()

					return ctx.Err()
				}
			}), 1)

		descriptor := newDescriptor()
		err := scheduler.Schedule(descriptor, false)
		require.NoError(t, err)
		require.Eventually(t, TaskActive(store, provider, descriptor), time.Second, 100*time.Millisecond)

		err = scheduler.Schedule(descriptor, false)
		require.Equal(t, ErrAlreadyScheduled, err)
	})

	t.Run("Error", func(t *testing.T) {
		t.Parallel()

		provider := models.ConnectorProvider(uuid.New().String())
		store := NewInMemoryStore()
		scheduler := NewDefaultScheduler(provider, logger, store, DefaultContainerFactory,
			ResolverFn(func(descriptor models.TaskDescriptor) Task {
				return func() error {
					return errors.New("test")
				}
			}), 1)

		descriptor := newDescriptor()
		err := scheduler.Schedule(descriptor, false)
		require.NoError(t, err)
		require.Eventually(t, TaskFailed(store, provider, descriptor, "test"), time.Second,
			100*time.Millisecond)
	})

	t.Run("Pending", func(t *testing.T) {
		t.Parallel()

		provider := models.ConnectorProvider(uuid.New().String())
		store := NewInMemoryStore()
		descriptor1 := newDescriptor()
		descriptor2 := newDescriptor()

		task1Terminated := make(chan struct{})
		task2Terminated := make(chan struct{})

		scheduler := NewDefaultScheduler(provider, logger, store, DefaultContainerFactory,
			ResolverFn(func(descriptor models.TaskDescriptor) Task {
				switch string(descriptor) {
				case string(descriptor1):
					return func(ctx context.Context) error {
						select {
						case <-task1Terminated:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				case string(descriptor2):
					return func(ctx context.Context) error {
						select {
						case <-task2Terminated:
							return nil
						case <-ctx.Done():
							return ctx.Err()
						}
					}
				}

				panic("unknown descriptor")
			}), 1)

		require.NoError(t, scheduler.Schedule(descriptor1, false))
		require.NoError(t, scheduler.Schedule(descriptor2, false))
		require.Eventually(t, TaskActive(store, provider, descriptor1), time.Second, 100*time.Millisecond)
		require.Eventually(t, TaskPending(store, provider, descriptor2), time.Second, 100*time.Millisecond)
		close(task1Terminated)
		require.Eventually(t, TaskTerminated(store, provider, descriptor1), time.Second, 100*time.Millisecond)
		require.Eventually(t, TaskActive(store, provider, descriptor2), time.Second, 100*time.Millisecond)
		close(task2Terminated)
		require.Eventually(t, TaskTerminated(store, provider, descriptor2), time.Second, 100*time.Millisecond)
	})

	t.Run("Stop scheduler", func(t *testing.T) {
		t.Parallel()

		provider := models.ConnectorProvider(uuid.New().String())
		store := NewInMemoryStore()
		mainDescriptor := newDescriptor()
		workerDescriptor := newDescriptor()

		scheduler := NewDefaultScheduler(provider, logger, store, DefaultContainerFactory,
			ResolverFn(func(descriptor models.TaskDescriptor) Task {
				switch string(descriptor) {
				case string(mainDescriptor):
					return func(ctx context.Context, scheduler Scheduler) {
						<-ctx.Done()
						require.NoError(t, scheduler.Schedule(workerDescriptor, false))
					}
				default:
					panic("should not be called")
				}
			}), 1)

		require.NoError(t, scheduler.Schedule(mainDescriptor, false))
		require.Eventually(t, TaskActive(store, provider, mainDescriptor), time.Second, 100*time.Millisecond)
		require.NoError(t, scheduler.Shutdown(context.Background()))
		require.Eventually(t, TaskTerminated(store, provider, mainDescriptor), time.Second, 100*time.Millisecond)
		require.Eventually(t, TaskPending(store, provider, workerDescriptor), time.Second, 100*time.Millisecond)
	})
}
