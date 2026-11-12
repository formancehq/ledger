package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

func TestCapabilityMembershipFenceHeldUntilPolicyApply(t *testing.T) {
	n := &Node{stopChannel: make(chan struct{}), terminalCh: make(chan struct{})}
	n.confChangeMu.Configure(nil, n.stopChannel, n.terminalCh)
	release, err := n.confChangeMu.AcquireClusterPolicy(context.Background())
	require.NoError(t, err)
	future := futures.New[state.ApplyResult]()
	release(future)
	require.False(t, n.confChangeMu.TryLock(), "a learner must not pass a pending activation")

	future.Resolve(state.ApplyResult{}, nil)
	require.Eventually(t, func() bool {
		if !n.confChangeMu.TryLock() {
			return false
		}
		n.confChangeMu.Unlock()

		return true
	}, time.Second, time.Millisecond)
}

func TestCapabilityMembershipFenceReleasedWithoutProposal(t *testing.T) {
	n := &Node{}
	release, err := n.confChangeMu.AcquireClusterPolicy(context.Background())
	require.NoError(t, err)
	release(nil)
	require.True(t, n.confChangeMu.TryLock())
	n.confChangeMu.Unlock()
}
