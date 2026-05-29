package state

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/pkg/signal"
)

func TestPeriodSchedulerStartStop(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	ps := NewPeriodScheduler(
		logger,
		func() bool { return true },
		func() string { return "" },
		func() {},
		signal.New(),
	)
	ps.Start()
	ps.Stop()
	// No deadlock or panic means success
}

func TestPeriodSchedulerEmptySchedule(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	var called atomic.Int32

	ps := NewPeriodScheduler(
		logger,
		func() bool { return true },
		func() string { return "" }, // empty schedule - should not fire
		func() { called.Add(1) },
		signal.New(),
	)
	ps.Start()

	// Verify nothing fires
	require.Never(t, func() bool { return called.Load() > 0 }, 200*time.Millisecond, 10*time.Millisecond, "empty schedule should never fire")

	ps.Stop()
}

func TestPeriodSchedulerInvalidCron(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	var called atomic.Int32

	ps := NewPeriodScheduler(
		logger,
		func() bool { return true },
		func() string { return "not-a-cron" }, // invalid cron
		func() { called.Add(1) },
		signal.New(),
	)
	ps.Start()

	// Verify nothing fires
	require.Never(t, func() bool { return called.Load() > 0 }, 200*time.Millisecond, 10*time.Millisecond, "invalid cron should never fire")

	ps.Stop()
}

func TestPeriodSchedulerScheduleChanged(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	sig := signal.New()

	var schedule atomic.Value
	schedule.Store("")

	ps := NewPeriodScheduler(
		logger,
		func() bool { return true },
		func() string { return schedule.Load().(string) },
		func() {},
		sig,
	)
	ps.Start()

	// Change schedule
	schedule.Store("*/1 * * * *")
	sig.Notify()

	// Allow the signal to be processed
	require.Never(t, func() bool { return false }, 100*time.Millisecond, 50*time.Millisecond)

	// Changing to empty should clear the timer
	schedule.Store("")
	sig.Notify()

	require.Never(t, func() bool { return false }, 100*time.Millisecond, 50*time.Millisecond)

	ps.Stop()
	// No panic or deadlock = success
}

func TestPeriodSchedulerNonLeaderDoesNotPropose(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	var called atomic.Int32

	// Use a cron that fires every second
	ps := NewPeriodScheduler(
		logger,
		func() bool { return false }, // not leader
		func() string { return "* * * * * *" },
		func() { called.Add(1) },
		signal.New(),
	)
	ps.Start()

	// Even with a fast-firing cron, non-leaders should not propose
	require.Never(t, func() bool { return called.Load() > 0 }, 300*time.Millisecond, 10*time.Millisecond, "non-leader should not propose")

	ps.Stop()
}
