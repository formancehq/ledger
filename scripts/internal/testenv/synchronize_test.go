package testenv

import (
	"context"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunSynchronizedTimeoutTerminatesAndReportsEveryPeer(t *testing.T) {
	first := exec.Command("/bin/sh", "-c", "echo first-peer-output >&2; printf 'ready\\n' >&3; IFS= read -r _ <&4")
	// Stop inside the peer after its diagnostic so reaping cannot race a forked sleep child.
	second := exec.Command("/bin/sh", "-c", "echo second-peer-output >&2; kill -STOP $$")
	deadline := make(chan time.Time, 1)
	var triggerDeadlineOnce sync.Once
	triggerDeadline := func() {
		triggerDeadlineOnce.Do(func() { deadline <- time.Now() })
	}
	outputWritten := make(chan string, 2)
	peerReady := make(chan string, 1)
	type runResult struct {
		result SynchronizedResult
		err    error
	}
	completed := make(chan runResult, 1)
	exited := make(chan struct{})
	t.Cleanup(func() {
		triggerDeadline()
		cleanupDeadline := time.NewTimer(2 * time.Second)
		defer cleanupDeadline.Stop()
		select {
		case <-exited:
		case <-cleanupDeadline.C:
			t.Error("timed out cleaning up synchronized fixture peers")
		}
	})
	go func() {
		defer close(exited)
		result, err := runSynchronized(200*time.Millisecond, synchronizationHooks{
			deadline:      deadline,
			outputWritten: func(name string) { outputWritten <- name },
			peerReady:     func(name string) { peerReady <- name },
		},
			SynchronizedCommand{Name: "first peer", Command: first},
			SynchronizedCommand{Name: "second peer", Command: second},
		)
		completed <- runResult{result: result, err: err}
	}()

	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	outputs := map[string]bool{}
	for len(outputs) < 2 {
		outputs[receiveSynchronizedEvent(t, ctx, outputWritten)] = true
	}
	require.Equal(t, map[string]bool{"first peer": true, "second peer": true}, outputs)
	require.Equal(t, "first peer", receiveSynchronizedEvent(t, ctx, peerReady))
	triggerDeadline()

	run := receiveSynchronizedEvent(t, ctx, completed)
	result, err := run.result, run.err
	require.Error(t, err)
	require.Less(t, result.Duration, 2*time.Second, err.Error())
	require.Contains(t, err.Error(), "fixture synchronization exceeded 200ms")
	require.Contains(t, err.Error(), "first peer stdout/stderr:\nfirst-peer-output")
	require.Contains(t, err.Error(), "second peer stdout/stderr:\nsecond-peer-output")
	_, firstReaped := result.Exit["first peer"]
	_, secondReaped := result.Exit["second peer"]
	require.True(t, firstReaped, "first peer was not reaped: %v", err)
	require.True(t, secondReaped, "second peer was not reaped: %v", err)
}

func receiveSynchronizedEvent[T any](t *testing.T, ctx context.Context, events <-chan T) T {
	t.Helper()

	select {
	case event := <-events:
		return event
	case <-ctx.Done():
		t.Fatalf("timed out waiting for synchronized fixture event: %v", ctx.Err())

		var zero T

		return zero
	}
}
