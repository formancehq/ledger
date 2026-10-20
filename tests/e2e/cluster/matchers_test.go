//go:build e2e

package cluster

import (
	"context"
	"fmt"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/onsi/gomega/types"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

type beFollowerMatcher struct{}

func (matcher beFollowerMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*testutil.ServiceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *testutil.ServiceWithClient, got %T", actual)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	clusterState, err := srv.ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{
		NodeId: srv.NodeID,
	})
	if err != nil {
		return false, nil // gRPC not ready yet — retry
	}

	if clusterState.Leader == 0 {
		return false, nil // Leader not yet known
	}
	return clusterState.Leader != clusterState.LocalNode, nil
}

func (matcher beFollowerMatcher) FailureMessage(_ any) (message string) {
	return "Expected node to be a follower"
}

func (matcher beFollowerMatcher) NegatedFailureMessage(_ any) (message string) {
	return "Expected node not to be a follower"
}

func BeFollower() types.GomegaMatcher {
	return beFollowerMatcher{}
}

var _ types.GomegaMatcher = (*beFollowerMatcher)(nil)

type haveALeaderMatcher struct {
	fetchTo *uint64
}

func (h haveALeaderMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*testutil.ServiceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *testutil.ServiceWithClient, got %T", actual)
	}

	clusterState, err := srv.ClusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{
		NodeId: srv.NodeID,
	})
	if err != nil {
		return false, nil // gRPC not ready yet — retry
	}

	if clusterState.Leader == 0 {
		return false, nil
	}

	leaderID := uint64(clusterState.Leader)
	if h.fetchTo != nil {
		*h.fetchTo = leaderID
	}

	return leaderID != 0, nil
}

func (h haveALeaderMatcher) FailureMessage(_ any) (message string) {
	return "Expected cluster to have a leader"
}

func (h haveALeaderMatcher) NegatedFailureMessage(_ any) (message string) {
	return "Expected cluster not to have a leader"
}

func HaveALeader(fetchTo *uint64) types.GomegaMatcher {
	return haveALeaderMatcher{
		fetchTo: fetchTo,
	}
}

var _ types.GomegaMatcher = (*haveALeaderMatcher)(nil)
