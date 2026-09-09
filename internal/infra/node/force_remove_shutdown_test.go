package node

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
)

func TestForceRemoveNodeAdmittedCommandStopsBeforeExecution(t *testing.T) {
	t.Parallel()

	n := &Node{
		clusterCommandCh: make(chan *clusterCommand, 1),
		terminalCh:       make(chan struct{}),
		runDone:          make(chan struct{}),
	}
	t.Cleanup(func() {
		select {
		case <-n.runDone:
		default:
			close(n.runDone)
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() { result <- n.ForceRemoveNode(ctx, 2) }()

	// Observe actual admission, then model Run stopping before orchestrate
	// executes the command. A nil rawNode ensures execution cannot be hidden.
	select {
	case <-n.clusterCommandCh:
	case <-time.After(time.Second):
		t.Fatal("force-removal was not admitted")
	}
	cancel()
	close(n.runDone)
	require.ErrorIs(t, receiveForceRemoveError(t, result), raft.ErrStopped)

	// The caller must release the shared configuration-change lock, allowing
	// later operations to observe shutdown instead of blocking behind it.
	locked := n.confChangeMu.TryLock()
	require.True(t, locked, "shutdown must release the configuration-change lock")
	n.confChangeMu.Unlock()
	require.ErrorIs(t, n.ForceRemoveNode(context.Background(), 3), raft.ErrStopped)
	require.Empty(t, n.clusterCommandCh, "an already stopped node must reject admission")
}

func TestClusterCommandResultAfterStopPreservesCompletedOutcome(t *testing.T) {
	t.Parallel()

	commandErr := errors.New("command completed with a validation error")
	terminalErr := &terminalNodeError{cause: errors.New("terminal persistence failure")}
	for _, tc := range []struct {
		name     string
		result   error
		terminal *terminalNodeError
		want     error
	}{
		{name: "successful result"},
		{name: "command error", result: commandErr, want: commandErr},
		{name: "terminal failure takes precedence", terminal: terminalErr, want: terminalErr},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			n := &Node{runDone: make(chan struct{})}
			cmd := &clusterCommand{errCh: make(chan error, 1)}
			cmd.errCh <- tc.result
			if tc.terminal != nil {
				n.terminalFailure.Store(tc.terminal)
			}
			close(n.runDone)

			// Invoke the shutdown branch directly with both signals ready;
			// scheduler choice cannot turn this into a result-only test.
			err := n.clusterCommandResultAfterStop(cmd)
			if tc.want == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.want)
			}
		})
	}
}
