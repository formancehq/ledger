//go:build e2e

package e2e

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/onsi/gomega/types"
)

type beFollowerMatcher struct{}

func (matcher beFollowerMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.GetClusterState(context.Background(), &servicepb.GetClusterStateRequest{})
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

type hasNextLogIDMatcher struct {
	ledgerName          string
	expectedNextLastLog uint64
	observedNextLastLog uint64
}

func (matcher *hasNextLogIDMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.GetClusterState(context.Background(), &servicepb.GetClusterStateRequest{})
	if err != nil {
		return false, err
	}

	// Find the ledger by name in the inner state
	if clusterState.InnerState == nil {
		return false, fmt.Errorf("inner state is nil")
	}
	for _, ledgerState := range clusterState.InnerState.Ledgers {
		if ledgerState.LedgerInfo != nil && ledgerState.LedgerInfo.Name == matcher.ledgerName {
			matcher.observedNextLastLog = ledgerState.NextLogId
			break
		}
	}

	if matcher.observedNextLastLog > matcher.expectedNextLastLog {
		return false, fmt.Errorf("last log %d is greater than expected %d", matcher.observedNextLastLog, matcher.expectedNextLastLog)
	}

	return matcher.observedNextLastLog == matcher.expectedNextLastLog, nil
}

func (matcher *hasNextLogIDMatcher) FailureMessage(_ any) (message string) {
	return fmt.Sprintf("Expected %s to have last log %d, got %d", matcher.ledgerName, matcher.expectedNextLastLog, matcher.observedNextLastLog)
}

func (matcher *hasNextLogIDMatcher) NegatedFailureMessage(_ any) (message string) {
	return fmt.Sprintf("Expected %s not to have last log %d", matcher.ledgerName, matcher.expectedNextLastLog)
}

func HasNextLogID(ledgerName string, lastLog uint64) types.GomegaMatcher {
	return &hasNextLogIDMatcher{
		ledgerName:          ledgerName,
		expectedNextLastLog: lastLog,
	}
}

var _ types.GomegaMatcher = (*hasNextLogIDMatcher)(nil)

type haveALeaderMatcher struct {
	fetchTo *uint64
}

func (h haveALeaderMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.GetClusterState(context.Background(), &servicepb.GetClusterStateRequest{})
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
