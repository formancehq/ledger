//go:build e2e

package e2e

import (
	"context"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/pkg/client/models/operations"
	"github.com/onsi/gomega/types"
)

type beLedgerFollowerMatcher struct {
	ledgerName string
}

func (matcher beLedgerFollowerMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.Ledgers.GetLedgerRaftState(context.Background(), operations.GetLedgerRaftStateRequest{
		LedgerName: matcher.ledgerName,
	})
	if err != nil {
		return false, err
	}

	if clusterState.LedgerClusterStateResponse.Data.Leader == nil {
		return false, nil
	}
	return *clusterState.LedgerClusterStateResponse.Data.Leader !=
		clusterState.LedgerClusterStateResponse.Data.LocalNode, nil
}

func (matcher beLedgerFollowerMatcher) FailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected node to be a follower for ledger '%s'", matcher.ledgerName)
}

func (matcher beLedgerFollowerMatcher) NegatedFailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected node not to be a follower for ledger '%s'", matcher.ledgerName)
}

func BeLedgerFollower(ledgerName string) types.GomegaMatcher {
	return beLedgerFollowerMatcher{
		ledgerName: ledgerName,
	}
}

var _ types.GomegaMatcher = (*beLedgerFollowerMatcher)(nil)

type beFollowerMatcher struct{}

func (matcher beFollowerMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.Cluster.GetClusterState(context.Background())
	if err != nil {
		return false, err
	}

	if clusterState.ClusterStateResponse.Data.Leader == nil {
		return false, nil
	}
	return *clusterState.ClusterStateResponse.Data.Leader !=
		clusterState.ClusterStateResponse.Data.LocalNode, nil
}

func (matcher beFollowerMatcher) FailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected node to be a follower")
}

func (matcher beFollowerMatcher) NegatedFailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected node not to be a follower")
}

func BeFollower() types.GomegaMatcher {
	return beFollowerMatcher{}
}

var _ types.GomegaMatcher = (*beFollowerMatcher)(nil)

type hasLastLogMatcher struct {
	ledgerName      string
	expectedLastLog uint64
	observedLastLog uint64
}

func (matcher *hasLastLogMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.Ledgers.GetLedgerRaftState(context.Background(), operations.GetLedgerRaftStateRequest{
		LedgerName: matcher.ledgerName,
	})
	if err != nil {
		return false, err
	}

	matcher.observedLastLog = uint64(clusterState.LedgerClusterStateResponse.Data.InnerState.LastLogID)

	if matcher.observedLastLog > matcher.expectedLastLog {
		return false, fmt.Errorf("last log %d is greater than expected %d", matcher.observedLastLog, matcher.expectedLastLog)
	}

	return matcher.observedLastLog == matcher.expectedLastLog, nil
}

func (matcher *hasLastLogMatcher) FailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected %s to have last log %d, got %d", matcher.ledgerName, matcher.expectedLastLog, matcher.observedLastLog)
}

func (matcher *hasLastLogMatcher) NegatedFailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected %s not to have last log %d", matcher.ledgerName, matcher.expectedLastLog)
}

func HasLastLog(ledgerName string, lastLog uint64) types.GomegaMatcher {
	return &hasLastLogMatcher{
		ledgerName:      ledgerName,
		expectedLastLog: lastLog,
	}
}

var _ types.GomegaMatcher = (*hasLastLogMatcher)(nil)

type haveALeaderMatcher struct {
	fetchTo *uint64
}

func (h haveALeaderMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.Cluster.GetClusterState(context.Background())
	if err != nil {
		return false, err
	}

	if clusterState.ClusterStateResponse.Data.Leader == nil {
		return false, nil
	}

	leaderID := uint64(*clusterState.ClusterStateResponse.Data.Leader)
	if h.fetchTo != nil {
		*h.fetchTo = leaderID
	}

	return leaderID != 0, nil
}

func (h haveALeaderMatcher) FailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected cluster to have a leader")
}

func (h haveALeaderMatcher) NegatedFailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected cluster not to have a leader")
}

func HaveALeader(fetchTo *uint64) types.GomegaMatcher {
	return haveALeaderMatcher{
		fetchTo: fetchTo,
	}
}

var _ types.GomegaMatcher = (*haveALeaderMatcher)(nil)


type haveALeaderOnLedgerMatcher struct {
	fetchTo *uint64
	ledgerName string
}

func (h haveALeaderOnLedgerMatcher) Match(actual any) (success bool, err error) {
	srv, ok := actual.(*serviceWithClient)
	if !ok {
		return false, fmt.Errorf("expected *serviceWithClient, got %T", actual)
	}

	clusterState, err := srv.client.Ledgers.GetLedgerRaftState(context.Background(), operations.GetLedgerRaftStateRequest{
		LedgerName: h.ledgerName,
	})
	if err != nil {
		return false, err
	}

	if clusterState.LedgerClusterStateResponse.Data.Leader == nil {
		return false, nil
	}

	leaderID := uint64(*clusterState.LedgerClusterStateResponse.Data.Leader)
	if h.fetchTo != nil {
		*h.fetchTo = leaderID
	}

	return leaderID != 0, nil
}

func (h haveALeaderOnLedgerMatcher) FailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected ledger cluster to have a leader")
}

func (h haveALeaderOnLedgerMatcher) NegatedFailureMessage(actual any) (message string) {
	return fmt.Sprintf("Expected ledger cluster not to have a leader")
}

func HaveALeaderOnLedger(ledgerName string, fetchTo *uint64) types.GomegaMatcher {
	return haveALeaderOnLedgerMatcher{
		fetchTo: fetchTo,
		ledgerName: ledgerName,
	}
}

var _ types.GomegaMatcher = (*haveALeaderOnLedgerMatcher)(nil)


