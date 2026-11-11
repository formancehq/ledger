package node

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/infra/state"
	"github.com/formancehq/ledger/v3/internal/pkg/futures"
)

func TestCapabilityMembershipFenceHeldUntilPolicyApply(t *testing.T) {
	n := &Node{stopChannel: make(chan struct{}), terminalCh: make(chan struct{})}
	n.confChangeMu.Lock() // acquired before the policy proposal is enqueued
	future := futures.New[state.ApplyResult]()
	n.releaseCapabilityMembershipAfter(future)
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
	n.confChangeMu.Lock()
	n.releaseCapabilityMembershipAfter(nil)
	require.True(t, n.confChangeMu.TryLock())
	n.confChangeMu.Unlock()
}
