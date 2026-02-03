//go:build e2e

package e2e

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/onsi/gomega/types"
)

type beFollowerMatcher struct{}

func (matcher beFollowerMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.clusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{})
	if err != nil {
		return false, fmt.Errorf("gRPC error getting cluster state: %w", err)
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
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.clusterClient.GetClusterState(context.Background(), &clusterpb.GetClusterStateRequest{})
	if err != nil {
		return false, err
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
