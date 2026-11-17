package node

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestFullProposalQueueRejectsRevokedLease(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.logger = logging.Testing()
	n.proposeCh = make(chan *Proposal, 1)
	n.proposeCh <- newProposal(1, []byte("occupies queue"), nil)
	n.indexTracker = NewIndexTracker(10)
	n.leadershipEnabled = true
	lease := n.gainLeadership(t.Context(), 1)
	proposal := newProposal(124, []byte("must not enqueue"), nil)
	result := make(chan error, 1)
	go func() {
		n.indexTracker.Lock()
		_, err := n.Propose(WithLeadershipLease(t.Context(), lease), proposal)
		n.indexTracker.Unlock()
		result <- err
	}()
	require.Eventually(t, func() bool {
		_, registered := n.applier.futures.Load(proposal.commandID)

		return registered
	}, time.Second, time.Millisecond)
	n.loseLeadership()
	require.ErrorIs(t, <-result, ErrNotLeader)
	require.Equal(t, uint64(10), n.indexTracker.Next())
	require.Len(t, n.proposeCh, 1)
	_, registered := n.applier.futures.Load(proposal.commandID)
	require.False(t, registered)
}

func TestSlowAdmissionKeepsCapturedLease(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.logger = logging.Testing()
	n.proposeCh = make(chan *Proposal, 1)
	n.indexTracker = NewIndexTracker(10)
	n.leadershipEnabled = true
	n.gainLeadership(t.Context(), 1)
	entered := make(chan struct{})
	release := make(chan struct{})
	n.proposalAdmission = func(context.Context, *Proposal) error {
		close(entered)
		<-release

		return nil
	}
	result := make(chan error, 1)
	go func() {
		n.indexTracker.Lock()
		_, err := n.Propose(t.Context(), newProposal(125, []byte("old tenure"), nil))
		n.indexTracker.Unlock()
		result <- err
	}()
	<-entered
	n.loseLeadership()
	n.gainLeadership(t.Context(), 2)
	close(release)
	require.ErrorIs(t, <-result, ErrNotLeader)
	require.Empty(t, n.proposeCh)
	require.Equal(t, uint64(10), n.indexTracker.Next())
}

func TestLeadershipLeaseOrdersAuthorizationAndRevocation(t *testing.T) {
	t.Parallel()
	lease := newLeadershipLease(context.Background(), 1, 7)
	registered := false
	require.True(t, lease.Authorize(func() { registered = true }))
	require.True(t, registered)
	lease.revoke()
	require.ErrorIs(t, lease.Context().Err(), context.Canceled)
	require.False(t, lease.Authorize(func() { t.Fatal("revoked lease authorized work") }))
}

func TestQueuedProposalRevokedBeforeAcceptance(t *testing.T) {
	t.Parallel()
	n, _ := newTestMaintenanceNode(t)
	n.logger = logging.Testing()
	n.proposeCh = make(chan *Proposal, 1)
	n.indexTracker = NewIndexTracker(10)
	n.leadershipEnabled = true
	lease := n.gainLeadership(t.Context(), 1)
	proposal := newProposal(123, []byte("never accepted"), nil)
	n.indexTracker.Lock()
	future, err := n.Propose(WithLeadershipLease(t.Context(), lease), proposal)
	n.indexTracker.Unlock()
	require.NoError(t, err)
	require.Same(t, proposal, <-n.proposeCh)
	require.Equal(t, uint64(11), n.indexTracker.Next())
	n.loseLeadership()
	// Revocation rejects without touching rawNode, even if Raft would appear
	// to be leader again at the dequeue point.
	n.rawNode = nil
	n.handleProposal(proposal)
	_, err = proposal.Wait(t.Context())
	require.ErrorIs(t, err, ErrNotLeader)
	_, err = future.Wait(t.Context())
	require.ErrorIs(t, err, ErrNotLeader)
	require.Equal(t, uint64(10), n.indexTracker.Next())
}

func TestLeadershipLeaseRevocationWaitsForAuthorization(t *testing.T) {
	t.Parallel()
	lease := newLeadershipLease(context.Background(), 1, 7)
	entered := make(chan struct{})
	release := make(chan struct{})
	authorized := make(chan bool, 1)
	go func() {
		authorized <- lease.Authorize(func() {
			close(entered)
			<-release
		})
	}()
	<-entered
	revoked := make(chan struct{})
	go func() { lease.revoke(); close(revoked) }()
	requireOpen(t, revoked)
	close(release)
	require.True(t, <-authorized)
	<-revoked
	require.ErrorIs(t, lease.Context().Err(), context.Canceled)
}
