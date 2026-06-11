package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestSingleTaskExecutorKeepsErrorAcrossRuns(t *testing.T) {
	t.Parallel()

	executor := NewSingleTaskExecutor(logging.FromContext(logging.TestingContext()))
	taskErr := errors.New("first task failed")

	executor.Run(context.Background(), func(ctx context.Context) error {
		return taskErr
	})
	executor.Interrupt()

	executor.Run(context.Background(), func(ctx context.Context) error {
		return nil
	})
	executor.Interrupt()

	var err error
	require.Eventually(t, func() bool {
		select {
		case err = <-executor.Error():
			return true
		default:
			return false
		}
	}, time.Second, 10*time.Millisecond)

	require.ErrorIs(t, err, taskErr)
}
