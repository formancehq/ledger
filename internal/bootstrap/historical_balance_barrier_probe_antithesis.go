//go:build antithesis

package bootstrap

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

const (
	antithesisHistoricalBalanceProbeMetadataKey          = "x-formance-antithesis-historical-balance-probe"
	antithesisHistoricalBalanceBarrierReachedMetadataKey = "x-formance-antithesis-historical-balance-barrier-reached"
	consistencyMetadataKey                             = "x-consistency"
	antithesisHistoricalBalanceBarrierTimeout            = time.Second
	antithesisHistoricalBalanceStateTimeout              = 500 * time.Millisecond
	antithesisHistoricalBalanceExpectedVoters            = 3
)

func antithesisHistoricalBalanceBarrierContext(ctx context.Context) (context.Context, context.CancelFunc) {
	if antithesisHistoricalBalanceProbeID(ctx) == "" {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, antithesisHistoricalBalanceBarrierTimeout)
}

// reachAntithesisHistoricalBalanceBarrierFailure is compiled only into the
// instrumented SUT. The workload omits x-consistency deliberately: reaching
// this point proves that the default route stopped at ReadIndexAndWait instead
// of falling through to the node-local primary and history stores.
func reachAntithesisHistoricalBalanceBarrierFailure(
	ctx context.Context,
	sutNode *node.Node,
	err error,
) {
	probeID := antithesisHistoricalBalanceProbeID(ctx)
	if probeID == "" {
		return
	}
	stateCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), antithesisHistoricalBalanceStateTimeout)
	state, stateErr := sutNode.GetClusterState(stateCtx)
	cancel()
	if stateErr != nil || state.GetLeader() != uint32(sutNode.GetNodeID()) {
		return
	}

	voterIDs := make([]uint32, 0, len(state.GetNodes()))
	for _, peer := range state.GetNodes() {
		if peer.GetSuffrage() == "Voter" {
			voterIDs = append(voterIDs, peer.GetId())
		}
	}
	if len(voterIDs) != antithesisHistoricalBalanceExpectedVoters {
		return
	}
	sort.Slice(voterIDs, func(left, right int) bool { return voterIDs[left] < voterIDs[right] })
	if headerErr := ggrpc.SendHeader(ctx, metadata.Pairs(
		antithesisHistoricalBalanceBarrierReachedMetadataKey,
		probeID,
	)); headerErr != nil {
		return
	}

	assert.Reachable(
		"historical balance: aggregate stopped at default linearizable read barrier",
		map[string]any{
			"probe_id":    probeID,
			"node_id":     sutNode.GetNodeID(),
			"leader_id":   state.GetLeader(),
			"voter_ids":   voterIDs,
			"consistency": "linearizable_default",
			"error_kind":  antithesisHistoricalBalanceBarrierErrorKind(err),
			"error":       fmt.Sprintf("%v", err),
		},
	)
}

func antithesisHistoricalBalanceProbeID(ctx context.Context) string {
	if len(metadata.ValueFromIncomingContext(ctx, consistencyMetadataKey)) != 0 {
		return ""
	}

	values := metadata.ValueFromIncomingContext(ctx, antithesisHistoricalBalanceProbeMetadataKey)
	if len(values) != 1 || values[0] == "" {
		return ""
	}

	return values[0]
}

func antithesisHistoricalBalanceBarrierErrorKind(err error) string {
	switch {
	case errors.Is(err, commonpb.ErrNoLeader):
		return "no_leader"
	case errors.Is(err, node.ErrNotLeader):
		return "not_leader"
	case errors.Is(err, node.ErrLeadershipLost):
		return "leadership_lost"
	case errors.Is(err, node.ErrNodeSyncing):
		return "node_syncing"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	case errors.Is(err, context.Canceled):
		return "canceled"
	default:
		return fmt.Sprintf("%T", err)
	}
}
