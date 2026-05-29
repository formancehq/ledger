package state

import (
	"sync/atomic"
	"testing"
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
		func() {},
		signal.New(),
	)
	s.Start()
	s.Stop()
	// No deadlock or panic means success
}

func TestQueryCheckpointSchedulerEmptySchedule(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	var called atomic.Int32

	s := NewQueryCheckpointScheduler(
		logger,
		func() bool { return true },
		func() string { return "" }, // empty schedule - should not fire
		func() { called.Add(1) },
		signal.New(),
	)
	s.Start()

	// Verify nothing fires
	require.Never(t, func() bool { return called.Load() > 0 }, 200*time.Millisecond, 10*time.Millisecond, "empty schedule should never fire")

	s.Stop()
}

func TestQueryCheckpointSchedulerInvalidCron(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	var called atomic.Int32

	s := NewQueryCheckpointScheduler(
		logger,
		func() bool { return true },
		func() string { return "not-a-cron" }, // invalid cron
		func() { called.Add(1) },
		signal.New(),
	)
	s.Start()

	// Verify nothing fires
	require.Never(t, func() bool { return called.Load() > 0 }, 200*time.Millisecond, 10*time.Millisecond, "invalid cron should never fire")

	s.Stop()
}

func TestQueryCheckpointSchedulerScheduleChanged(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	sig := signal.New()

	var schedule atomic.Value
	schedule.Store("")

	s := NewQueryCheckpointScheduler(
		logger,
		func() bool { return true },
		func() string { return schedule.Load().(string) },
		func() {},
		sig,
	)
	s.Start()

	// Change schedule
	schedule.Store("*/1 * * * *")
	sig.Notify()

	// Allow the signal to be processed
	require.Never(t, func() bool { return false }, 100*time.Millisecond, 50*time.Millisecond)

	// Changing to empty should clear the timer
	schedule.Store("")
	sig.Notify()

	require.Never(t, func() bool { return false }, 100*time.Millisecond, 50*time.Millisecond)

	s.Stop()
	// No panic or deadlock = success
}

func TestQueryCheckpointSchedulerNonLeaderDoesNotPropose(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	var called atomic.Int32

	// Use a cron that fires every second
	s := NewQueryCheckpointScheduler(
		logger,
		func() bool { return false }, // not leader
		func() string { return "* * * * * *" },
		func() { called.Add(1) },
		signal.New(),
	)
	s.Start()

	// Even with a fast-firing cron, non-leaders should not propose
	require.Never(t, func() bool { return called.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond, "non-leader should not propose")

	s.Stop()
}
