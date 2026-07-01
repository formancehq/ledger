package tailworker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func testLogger() logging.Logger { return logging.NopZap() }

func TestBootRunsOnceThenTicks(t *testing.T) {
	t.Parallel()

	var boots, ticks atomic.Int32

	tw := New(Config{
		Name:   "test",
		Logger: testLogger(),
		Ticker: 5 * time.Millisecond,
		Boot:   func(context.Context) error { boots.Add(1); return nil },
		Tick:   func(context.Context) error { ticks.Add(1); return nil },
	})
	tw.Start()
	t.Cleanup(tw.Stop)

	require.Eventually(t, func() bool { return ticks.Load() >= 3 }, time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), boots.Load(), "Boot must run exactly once")
}

func TestBootErrorAbortsLoop(t *testing.T) {
	t.Parallel()

	var ticks atomic.Int32

	tw := New(Config{
		Name:   "test",
		Logger: testLogger(),
		Ticker: 5 * time.Millisecond,
		Boot:   func(context.Context) error { return errors.New("boom") },
		Tick:   func(context.Context) error { ticks.Add(1); return nil },
	})
	tw.Start()
	t.Cleanup(tw.Stop)

	require.Never(t, func() bool { return ticks.Load() > 0 }, 100*time.Millisecond, 10*time.Millisecond)
}

func TestTickCanceledErrorSwallowedOthersContinue(t *testing.T) {
	t.Parallel()

	var ticks atomic.Int32

	tw := New(Config{
		Name:   "test",
		Logger: testLogger(),
		Ticker: 5 * time.Millisecond,
		Tick: func(context.Context) error {
			ticks.Add(1)
			return errors.New("transient")
		},
	})
	tw.Start()
	t.Cleanup(tw.Stop)

	require.Eventually(t, func() bool { return ticks.Load() >= 3 }, time.Second, 5*time.Millisecond)
}

func TestWakeTriggersTick(t *testing.T) {
	t.Parallel()

	wake := make(chan struct{}, 1)
	var ticks atomic.Int32

	tw := New(Config{
		Name:   "test",
		Logger: testLogger(),
		Ticker: time.Hour,
		Wake:   wake,
		Tick:   func(context.Context) error { ticks.Add(1); return nil },
	})
	tw.Start()
	t.Cleanup(tw.Stop)

	wake <- struct{}{}
	require.Eventually(t, func() bool { return ticks.Load() >= 1 }, time.Second, 5*time.Millisecond)
}

func TestStopReturnsWhileTickBlocksOnCtx(t *testing.T) {
	t.Parallel()

	entered := make(chan struct{}, 1)

	tw := New(Config{
		Name:   "test",
		Logger: testLogger(),
		Ticker: 5 * time.Millisecond,
		Tick: func(ctx context.Context) error {
			select {
			case entered <- struct{}{}:
			default:
			}
			<-ctx.Done()
			return ctx.Err()
		},
	})
	tw.Start()

	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("Tick never ran")
	}

	done := make(chan struct{})
	go func() { tw.Stop(); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Stop did not return — ctx not propagated to Tick")
	}
}
