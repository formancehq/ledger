package tailworker

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func testLogger() logging.Logger { return logging.NopZap() }

func TestBootRunsOnceThenTicks(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
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

		time.Sleep(17 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int32(3), ticks.Load())
		require.Equal(t, int32(1), boots.Load(), "Boot must run exactly once")
	})
}

func TestBootErrorRetriesThenTicks(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
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

		// The failed Boot parks the loop in its 100ms backoff: no tick yet.
		synctest.Wait()
		require.Equal(t, int32(1), boots.Load())
		require.Zero(t, ticks.Load())

		// Boot retries at 100ms, then the ticker fires once at 105ms.
		time.Sleep(106 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int32(1), ticks.Load())
		require.Equal(t, int32(2), boots.Load(), "Boot must recover without restarting the worker")
	})
}

func TestStopCancelsBootBackoff(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
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

		start := time.Now()
		done := make(chan struct{})
		go func() { tw.Stop(); close(done) }()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("Stop did not cancel the boot retry backoff")
		}
		// Stop returned without waiting out the 100ms backoff on the fake clock.
		require.Zero(t, time.Since(start))
	})
}

func TestTickCanceledErrorSwallowedOthersContinue(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
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

		time.Sleep(17 * time.Millisecond)
		synctest.Wait()
		require.Equal(t, int32(3), ticks.Load())
	})
}

func TestWakeTriggersTick(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
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
		synctest.Wait()
		require.Equal(t, int32(1), ticks.Load())
	})
}

func TestStopReturnsWhileTickBlocksOnCtx(t *testing.T) {
	t.Parallel()

	synctest.Test(t, func(t *testing.T) {
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
	})
}
