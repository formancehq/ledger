package reconciler

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestReconcilerTicksUntilStopped(t *testing.T) {
	t.Parallel()

	var count atomic.Int32
	r := New(func(context.Context) { count.Add(1) }, 5*time.Millisecond)
	r.Start()

	require.Eventually(t, func() bool { return count.Load() >= 3 }, 2*time.Second, 5*time.Millisecond,
		"reconcile must run repeatedly on the ticker")

	r.Stop()
	settled := count.Load()
	require.Never(t, func() bool { return count.Load() > settled }, 100*time.Millisecond, 10*time.Millisecond,
		"no reconcile must run after Stop")
}

func TestReconcilerTriggerIsCoalescedAndRaceSafe(t *testing.T) {
	t.Parallel()

	var count atomic.Int32
	r := New(func(context.Context) { count.Add(1) }, time.Hour)
	r.Start()
	require.Eventually(t, func() bool { return count.Load() >= 1 }, time.Second, time.Millisecond)

	var wg sync.WaitGroup
	for range 100 {
		wg.Go(func() {
			r.Trigger()
		})
	}
	wg.Wait()
	require.Eventually(t, func() bool { return count.Load() >= 2 }, time.Second, time.Millisecond,
		"an event must wake reconciliation without waiting for the ticker")
	r.Stop()
}

func TestNewRejectsNonPositiveInterval(t *testing.T) {
	t.Parallel()

	require.Panics(t, func() {
		New(func(context.Context) {}, 0)
	})
}
