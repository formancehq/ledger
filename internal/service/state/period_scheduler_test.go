package state

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/service/signal"
	"github.com/stretchr/testify/require"
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

	// Wait a bit to ensure nothing fires
	time.Sleep(200 * time.Millisecond)

	ps.Stop()
	require.Equal(t, int32(0), called.Load(), "empty schedule should never fire")
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

	// Wait a bit to ensure nothing fires
	time.Sleep(200 * time.Millisecond)

	ps.Stop()
	require.Equal(t, int32(0), called.Load(), "invalid cron should never fire")
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
	time.Sleep(100 * time.Millisecond)

	// Changing to empty should clear the timer
	schedule.Store("")
	sig.Notify()

	time.Sleep(100 * time.Millisecond)

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
	time.Sleep(300 * time.Millisecond)

	ps.Stop()
	require.Equal(t, int32(0), called.Load(), "non-leader should not propose")
}
