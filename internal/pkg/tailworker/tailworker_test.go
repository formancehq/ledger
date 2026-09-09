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
		Boot: func(context.Context) error {
			boots.Add(1)

			return nil
		},
		Tick: func(context.Context) error {
			ticks.Add(1)

			return nil
		},
	})
	tw.Start()
	t.Cleanup(tw.Stop)

	require.Eventually(t, func() bool { return ticks.Load() >= 3 }, time.Second, 5*time.Millisecond)
	require.Equal(t, int32(1), boots.Load(), "Boot must run exactly once")
}

func TestBootErrorRetriesThenTicks(t *testing.T) {
	t.Parallel()

	var boots atomic.Int32
	var ticks atomic.Int32

	tw := New(Config{
		Name:   "test",
		Logger: testLogger(),
		Ticker: 5 * time.Millisecond,
		Boot: func(context.Context) error {
			if boots.Add(1) == 1 {
				return errors.New("transient")
			}

			return nil
		},
		Tick: func(context.Context) error {
			ticks.Add(1)

			return nil
		},
	})
	tw.Start()
	t.Cleanup(tw.Stop)

	require.Eventually(t, func() bool { return ticks.Load() > 0 }, time.Second, 5*time.Millisecond)
	require.Equal(t, int32(2), boots.Load(), "Boot must recover without restarting the worker")
}

func TestStopCancelsBootBackoff(t *testing.T) {
	t.Parallel()

	booted := make(chan struct{}, 1)
	tw := New(Config{
		Name:   "test",
		Logger: testLogger(),
		Ticker: time.Hour,
		Boot: func(context.Context) error {
			booted <- struct{}{}

			return errors.New("persistent")
		},
		Tick: func(context.Context) error { return nil },
	})
	tw.Start()

	select {
	case <-booted:
	case <-time.After(time.Second):
		t.Fatal("Boot never ran")
	}

	done := make(chan struct{})
	go func() { tw.Stop(); close(done) }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not cancel the boot retry backoff")
	}
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
		Tick: func(context.Context) error {
			ticks.Add(1)

			return nil
		},
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
