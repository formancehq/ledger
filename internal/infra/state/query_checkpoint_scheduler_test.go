package state

import (
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/signal"
)

func TestQueryCheckpointSchedulerStartStop(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	s := NewQueryCheckpointScheduler(
		logger,
		func() bool { return true },
		func() string { return "" },
		func() error { return nil },
		signal.New(),
	)
	s.Start()
	s.Stop()
	// No deadlock or panic means success
}

func TestQueryCheckpointSchedulerEmptySchedule(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)

		var called atomic.Int32

		s := NewQueryCheckpointScheduler(
			logger,
			func() bool { return true },
			func() string { return "" }, // empty schedule - should not fire
			func() error {
				called.Add(1)

				return nil
			},
			signal.New(),
		)
		s.Start()

		// Verify nothing fires across a day of fake time.
		time.Sleep(24 * time.Hour)
		synctest.Wait()
		require.Zero(t, called.Load(), "empty schedule should never fire")

		s.Stop()
	})
}

func TestQueryCheckpointSchedulerInvalidCron(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)

		var called atomic.Int32

		s := NewQueryCheckpointScheduler(
			logger,
			func() bool { return true },
			func() string { return "not-a-cron" }, // invalid cron
			func() error {
				called.Add(1)

				return nil
			},
			signal.New(),
		)
		s.Start()

		// Verify nothing fires across a day of fake time.
		time.Sleep(24 * time.Hour)
		synctest.Wait()
		require.Zero(t, called.Load(), "invalid cron should never fire")

		s.Stop()
	})
}

func TestQueryCheckpointSchedulerScheduleChanged(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)
		sig := signal.New()

		var (
			schedule atomic.Value
			called   atomic.Int32
		)
		schedule.Store("")

		s := NewQueryCheckpointScheduler(
			logger,
			func() bool { return true },
			func() string { return schedule.Load().(string) },
			func() error {
				called.Add(1)

				return nil
			},
			sig,
		)
		s.Start()

		// Change schedule: the scheduler re-arms and fires on the next minute.
		schedule.Store("*/1 * * * *")
		sig.Notify()

		time.Sleep(90 * time.Second)
		synctest.Wait()
		require.Equal(t, int32(1), called.Load(), "new schedule must arm the timer")

		// Changing to empty should clear the timer
		schedule.Store("")
		sig.Notify()

		time.Sleep(time.Hour)
		synctest.Wait()
		require.Equal(t, int32(1), called.Load(), "cleared schedule must not fire")

		s.Stop()
	})
}

func TestQueryCheckpointSchedulerNonLeaderDoesNotPropose(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
		ctx := logging.TestingContext()
		logger := logging.FromContext(ctx)

		var called, leaderChecks atomic.Int32

		// Use a cron that fires every second
		s := NewQueryCheckpointScheduler(
			logger,
			func() bool {
				leaderChecks.Add(1)

				return false // not leader
			},
			func() string { return "* * * * * *" },
			func() error {
				called.Add(1)

				return nil
			},
			signal.New(),
		)
		s.Start()

		// Even with a fast-firing cron, non-leaders should not propose
		time.Sleep(10*time.Second + 500*time.Millisecond)
		synctest.Wait()
		require.Equal(t, int32(10), leaderChecks.Load(), "the cron must have fired every second")
		require.Zero(t, called.Load(), "non-leader should not propose")

		s.Stop()
	})
}
