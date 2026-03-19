package worker

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/go-libs/v4/logging"
)

func TestWorkerRunStop(t *testing.T) {
	t.Parallel()

	w := New()

	var ran atomic.Bool

	w.Run(func(stop <-chan struct{}) {
		ran.Store(true)
		<-stop
	})

	require.Eventually(t, ran.Load, time.Second, 10*time.Millisecond)
	w.Stop()
}

func TestWorkerStopBlocksUntilDone(t *testing.T) {
	t.Parallel()

	w := New()
	released := make(chan struct{})

	w.Run(func(stop <-chan struct{}) {
		<-stop
		// Simulate cleanup delay
		<-released
	})

	done := make(chan struct{})

	go func() {
		w.Stop()
		close(done)
	}()

	// Stop should block while fn hasn't returned
	select {
	case <-done:
		t.Fatal("Stop returned before fn finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(released)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Stop did not return after fn finished")
	}
}

func TestRetryWithBackoffSuccess(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())

	var calls atomic.Int32

	stop := make(chan struct{})

	RetryWithBackoff(stop, logger, func() error {
		calls.Add(1)

		return nil
	})

	require.Equal(t, int32(1), calls.Load())
}

func TestRetryWithBackoffRetries(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())

	var calls atomic.Int32

	stop := make(chan struct{})

	RetryWithBackoff(stop, logger, func() error {
		n := calls.Add(1)
		if n < 3 {
			return errors.New("transient error")
		}

		return nil
	})

	require.Equal(t, int32(3), calls.Load())
}

func TestRetryWithBackoffErrNotLeader(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())

	var calls atomic.Int32

	stop := make(chan struct{})

	RetryWithBackoff(stop, logger, func() error {
		n := calls.Add(1)
		if n < 3 {
			return ErrNotLeader
		}

		return nil
	})

	require.Equal(t, int32(3), calls.Load())
}

func TestRetryWithBackoffStop(t *testing.T) {
	t.Parallel()

	logger := logging.FromContext(logging.TestingContext())

	stop := make(chan struct{})

	var calls atomic.Int32

	done := make(chan struct{})

	go func() {
		RetryWithBackoff(stop, logger, func() error {
			calls.Add(1)

			return errors.New("permanent error")
		})
		close(done)
	}()

	// Let it retry at least once
	require.Eventually(t, func() bool {
		return calls.Load() >= 2
	}, 2*time.Second, 10*time.Millisecond)

	close(stop)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RetryWithBackoff did not stop")
	}
}

func TestDrainChannel(t *testing.T) {
	t.Parallel()

	ch := make(chan int, 3)
	ch <- 1

	ch <- 2

	ch <- 3

	var sum atomic.Int32

	stop := make(chan struct{})

	done := make(chan struct{})

	go func() {
		DrainChannel(stop, ch, func(v int) {
			sum.Add(int32(v))
		})
		close(done)
	}()

	require.Eventually(t, func() bool {
		return sum.Load() == 6
	}, time.Second, 10*time.Millisecond)

	close(stop)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("DrainChannel did not stop")
	}
}

func TestRunTicker(t *testing.T) {
	t.Parallel()

	var calls atomic.Int32

	stop := make(chan struct{})

	done := make(chan struct{})

	go func() {
		RunTicker(stop, 10*time.Millisecond, func() {
			calls.Add(1)
		})
		close(done)
	}()

	require.Eventually(t, func() bool {
		return calls.Load() >= 3
	}, time.Second, 10*time.Millisecond)

	close(stop)

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("RunTicker did not stop")
	}
}
