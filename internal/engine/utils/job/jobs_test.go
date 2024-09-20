package job

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/formancehq/go-libs/logging"
	"github.com/stretchr/testify/require"
)

func TestWorkerPool(t *testing.T) {
	t.Parallel()

	const countJobs = 10000
	createdJobs := atomic.Int64{}
	terminatedJobs := atomic.Int64{}
	nextJob := func() *builtJob {
		if createdJobs.Load() == 10000 {
			return nil
		}
		createdJobs.Add(1)
		return newJob(func() {
			terminatedJobs.Add(1)
		})
	}
	runner := func(ctx context.Context, job *builtJob) error {
		return nil
	}
	ctx := logging.TestingContext()

	pool := NewJobRunner[builtJob](runner, nextJob, 5)
	go pool.Run(ctx)
	defer pool.Close()

	for i := 0; i < 100; i++ {
		go pool.Next() // Simulate random input
	}

	require.Eventually(t, func() bool {
		return countJobs == createdJobs.Load()
	}, 5*time.Second, time.Millisecond*100)
}
