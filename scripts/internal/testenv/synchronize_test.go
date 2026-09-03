package testenv

import (
	"os/exec"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestRunSynchronizedTimeoutTerminatesAndReportsEveryPeer(t *testing.T) {
	first := exec.Command("/bin/sh", "-c", "echo first-peer-output >&2; printf 'ready\\n' >&3; IFS= read -r _ <&4")
	second := exec.Command("/bin/sh", "-c", "echo second-peer-output >&2; sleep 60")

	result, err := RunSynchronized(t, 200*time.Millisecond,
		SynchronizedCommand{Name: "first peer", Command: first},
		SynchronizedCommand{Name: "second peer", Command: second},
	)
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
