package state

import (
	"context"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestWaitForApplied_FastPath: the index has already been published when
// WaitForApplied is called → no blocking, no goroutine spawn, returns nil.
func TestWaitForApplied_FastPath(t *testing.T) {
	t.Parallel()

	fsm, _, _ := newTestMachine(t)
	fsm.publishApplied(42)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	require.NoError(t, fsm.WaitForApplied(ctx, 42))
	require.NoError(t, fsm.WaitForApplied(ctx, 41))
}

// TestWaitForApplied_LatePublish: the waiter starts before the publish.
// publishApplied takes appliedMu around Store + Broadcast, so this works
// regardless of which side wins the lock first. The pre-fix code (Store +
// Broadcast outside the lock) could publish in the window between the
// waiter's lastPersistedIndex.Load() check and its Cond.Wait() call —
// the broadcast then lands on an empty wait queue and the waiter sleeps
// until ctx times out (#327).
func TestWaitForApplied_LatePublish(t *testing.T) {
	t.Parallel()

	fsm, _, _ := newTestMachine(t)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- fsm.WaitForApplied(ctx, 7) }()

	// Give the waiter time to enter Cond.Wait so we exercise the wake-up
	// path rather than the fast path. The exact moment doesn't matter for
	// correctness, only the order: this test is about whether the wake-up
	// is delivered, not about racing the wake-up against the Load check.
	time.Sleep(10 * time.Millisecond)
	fsm.publishApplied(7)

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("WaitForApplied hung after publishApplied — lost wakeup (#327)")
	}
}

// TestWaitForApplied_NoLostWakeupUnderContention: many concurrent waiters
// vs many publishes. Every published index N must wake every waiter whose
// target <= N. The test runs 200 publishes paired with 200 waiters with
// staggered targets; each waiter must return inside its own timeout. With
// the pre-fix code, the unsynchronised Store + Broadcast pair drops
// wake-ups under contention and the test deadlines.
func TestWaitForApplied_NoLostWakeupUnderContention(t *testing.T) {
	t.Parallel()

	fsm, _, _ := newTestMachine(t)

	const targets = 200

	var (
		wg      sync.WaitGroup
		hangs   atomic.Int64
		ctxErrs atomic.Int64
		wakeups atomic.Int64
		ctx, cn = context.WithTimeout(context.Background(), 5*time.Second)
	)
	defer cn()

	for i := uint64(1); i <= targets; i++ {
		wg.Go(func() {
			waitCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
			defer cancel()

			if err := fsm.WaitForApplied(waitCtx, i); err != nil {
				if waitCtx.Err() != nil {
					hangs.Add(1)
				} else {
					ctxErrs.Add(1)
				}

				return
			}
			wakeups.Add(1)
		})
	}

	// Yield once so most waiters reach Cond.Wait before we start publishing,
	// keeping the writer on the wake-up path rather than the fast path.
	runtime.Gosched()

	go func() {
		for i := uint64(1); i <= targets; i++ {
			fsm.publishApplied(i)
		}
	}()

	wg.Wait()

	require.Zero(t, hangs.Load(),
		"%d waiters timed out → lost wakeup (#327)", hangs.Load())
	require.Zero(t, ctxErrs.Load(),
		"%d waiters returned an unexpected error", ctxErrs.Load())
	require.Equal(t, int64(targets), wakeups.Load(),
		"expected %d wake-ups, got %d", targets, wakeups.Load())
}
