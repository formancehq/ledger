package internal

import (
	"context"
	"fmt"
	"reflect"
	"sort"
	"time"

	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

const (
	pitConvergenceQuiescenceAttempts = 20
	pitConvergenceIndexTimeout       = 60 * time.Second
	pitConvergenceRPCTimeout         = 5 * time.Second
	pitConvergencePollInterval       = time.Second
	pitConvergenceMembershipDelay    = 250 * time.Millisecond
)

type pitRaftMemberSnapshot struct {
	id             uint32
	suffrage       string
	serviceAddress string
}

type pitRaftMembershipSnapshot struct {
	leader  uint32
	members []pitRaftMemberSnapshot
}

type pitConvergenceMemberConn struct {
	member pitRaftMemberSnapshot
	conn   *PerNodeConn
}

// WaitForQuiescentPITConvergence drives the complete P0 liveness proof. It
// keeps every voter and learner from one stable leader membership in the
// denominator and restarts the proof whenever membership or the exact durable
// Raft index moves.
func WaitForQuiescentPITConvergence(
	ctx context.Context,
	driver servicepb.BucketServiceClient,
	clusterClient clusterpb.ClusterServiceClient,
) (bool, Details) {
	details := Details{
		"ledger":                   PITConvergenceLedgerName(),
		"expected_monetary_oracle": PITConvergenceExpectedVolumes(),
	}
	markerLogSequence := waitForPITConvergenceMarker(ctx, driver)
	details["marker_log_sequence"] = markerLogSequence
	if markerLogSequence == 0 {
		details["last_error"] = "post-fault convergence marker was never acknowledged"

		return false, details
	}

	ledgerID := waitForPITConvergenceLedgerID(ctx, driver)
	details["ledger_id"] = ledgerID
	if ledgerID == 0 {
		details["last_error"] = "PIT convergence ledger incarnation was never resolved"

		return false, details
	}

	for ctx.Err() == nil {
		target, err := WaitForQuiescentCommitIndex(
			ctx,
			driver,
			pitConvergenceQuiescenceAttempts,
		)
		if err != nil || target == 0 {
			details["last_error"] = fmt.Sprintf("quiescence barrier did not stabilize: %v", err)
			if !waitForPITConvergenceRetry(ctx) {
				break
			}

			continue
		}

		before, stable := stablePITRaftMembership(ctx, clusterClient)
		if !stable {
			details["last_error"] = "leader membership was not stable before PIT probes"
			if !waitForPITConvergenceRetry(ctx) {
				break
			}

			continue
		}
		details["target_commit_index"] = target
		details["membership_before"] = pitMembershipDetails(before)

		dialCtx, cancelDial := context.WithTimeout(ctx, pitConvergenceRPCTimeout)
		perNode, dialErr := DialPerNode(dialCtx, false)
		cancelDial()
		if dialErr != nil {
			details["last_error"] = fmt.Sprintf("dialing direct PIT members: %v", dialErr)
			if !waitForPITConvergenceRetry(ctx) {
				break
			}

			continue
		}

		members, resolved := resolvePITConvergenceMembers(before, perNode)
		if !resolved {
			details["last_error"] = "not every stable Raft member had one direct connection"
			perNode.Close()
			if !waitForPITConvergenceRetry(ctx) {
				break
			}

			continue
		}

		atIndex, indexDetails := waitPITConvergenceMembersAtExactIndex(ctx, members, target)
		details["member_index_status"] = indexDetails
		if !atIndex {
			details["last_error"] = "not every stable Raft member reached the exact durable index"
			perNode.Close()
			if !waitForPITConvergenceRetry(ctx) {
				break
			}

			continue
		}

		for ctx.Err() == nil {
			current, currentErr := readPITRaftMembership(ctx, clusterClient)
			if currentErr != nil {
				details["last_error"] = fmt.Sprintf("re-reading membership during PIT recovery: %v", currentErr)
				if !waitForPITConvergenceRetry(ctx) {
					break
				}

				continue
			}
			if !samePITRaftMembership(before, current) {
				details["last_error"] = "Raft membership or leader changed during PIT recovery"

				break
			}

			exact, observationDetails := observeEveryPITConvergenceMember(
				ctx,
				members,
				ledgerID,
				markerLogSequence,
			)
			details["member_pit_status"] = observationDetails
			if !exact {
				details["last_error"] = "one or more stable Raft members did not serve the exact PIT oracle"
				if !waitForPITConvergenceRetry(ctx) {
					break
				}

				continue
			}

			stillAtIndex, postPITIndexDetails := waitPITConvergenceMembersAtExactIndex(ctx, members, target)
			details["member_index_status_after_pit"] = postPITIndexDetails
			if !stillAtIndex {
				details["last_error"] = "a Raft member left the exact durable index during PIT observations"

				break
			}

			after, stableAfter := stablePITRaftMembership(ctx, clusterClient)
			if !stableAfter || !samePITRaftMembership(before, after) {
				details["last_error"] = "Raft membership changed after PIT observations"

				break
			}
			details["membership_after"] = pitMembershipDetails(after)

			barrierCtx, cancelBarrier := context.WithTimeout(ctx, pitConvergenceRPCTimeout)
			afterIndex, barrierErr := BarrierCommitIndex(barrierCtx, driver)
			cancelBarrier()
			details["final_commit_index"] = afterIndex
			if barrierErr == nil && afterIndex == target+1 {
				perNode.Close()

				return true, details
			}

			details["last_error"] = fmt.Sprintf(
				"final barrier returned index=%d error=%v, expected index=%d",
				afterIndex,
				barrierErr,
				target+1,
			)

			break
		}

		perNode.Close()
		if !waitForPITConvergenceRetry(ctx) {
			break
		}
	}

	return false, details
}

func waitForPITConvergenceMarker(
	ctx context.Context,
	client servicepb.BucketServiceClient,
) uint64 {
	for ctx.Err() == nil {
		callCtx, cancel := context.WithTimeout(ctx, pitConvergenceRPCTimeout)
		sequence, err := CommitPITConvergencePostFaultMarker(callCtx, client)
		cancel()
		if err == nil {
			return sequence
		}

		if !waitForPITConvergenceRetry(ctx) {
			break
		}
	}

	return 0
}

func waitForPITConvergenceLedgerID(
	ctx context.Context,
	client servicepb.BucketServiceClient,
) uint32 {
	for ctx.Err() == nil {
		callCtx, cancel := context.WithTimeout(ctx, pitConvergenceRPCTimeout)
		ledger, err := client.GetLedger(callCtx, &servicepb.GetLedgerRequest{
			Ledger: PITConvergenceLedgerName(),
		})
		cancel()
		if err == nil && ledger.GetId() != 0 {
			return ledger.GetId()
		}

		if !waitForPITConvergenceRetry(ctx) {
			break
		}
	}

	return 0
}

func stablePITRaftMembership(
	ctx context.Context,
	client clusterpb.ClusterServiceClient,
) (pitRaftMembershipSnapshot, bool) {
	first, err := readPITRaftMembership(ctx, client)
	if err != nil || !waitForPITConvergenceDelay(ctx, pitConvergenceMembershipDelay) {
		return pitRaftMembershipSnapshot{}, false
	}
	second, err := readPITRaftMembership(ctx, client)
	if err != nil || !samePITRaftMembership(first, second) {
		return pitRaftMembershipSnapshot{}, false
	}

	return second, true
}

func readPITRaftMembership(
	ctx context.Context,
	client clusterpb.ClusterServiceClient,
) (pitRaftMembershipSnapshot, error) {
	callCtx, cancel := context.WithTimeout(ctx, pitConvergenceRPCTimeout)
	state, err := client.GetClusterState(callCtx, &clusterpb.GetClusterStateRequest{})
	cancel()
	if err != nil {
		return pitRaftMembershipSnapshot{}, err
	}
	if state.GetLeader() == 0 || len(state.GetNodes()) == 0 {
		return pitRaftMembershipSnapshot{}, fmt.Errorf(
			"incomplete leader membership leader=%d members=%d",
			state.GetLeader(),
			len(state.GetNodes()),
		)
	}

	members := make([]pitRaftMemberSnapshot, 0, len(state.GetNodes()))
	seenIDs := make(map[uint32]struct{}, len(state.GetNodes()))
	seenAddresses := make(map[string]struct{}, len(state.GetNodes()))
	for _, member := range state.GetNodes() {
		id := member.GetId()
		address := member.GetServiceAddress()
		if id == 0 || address == "" {
			return pitRaftMembershipSnapshot{}, fmt.Errorf(
				"membership contains incomplete node id=%d address=%q",
				id,
				address,
			)
		}
		if _, exists := seenIDs[id]; exists {
			return pitRaftMembershipSnapshot{}, fmt.Errorf("membership repeats node ID %d", id)
		}
		if _, exists := seenAddresses[address]; exists {
			return pitRaftMembershipSnapshot{}, fmt.Errorf("membership repeats service address %q", address)
		}
		seenIDs[id] = struct{}{}
		seenAddresses[address] = struct{}{}
		members = append(members, pitRaftMemberSnapshot{
			id:             id,
			suffrage:       member.GetSuffrage(),
			serviceAddress: address,
		})
	}
	sort.Slice(members, func(left, right int) bool { return members[left].id < members[right].id })

	return pitRaftMembershipSnapshot{
		leader:  state.GetLeader(),
		members: members,
	}, nil
}

func samePITRaftMembership(
	left pitRaftMembershipSnapshot,
	right pitRaftMembershipSnapshot,
) bool {
	return left.leader == right.leader && reflect.DeepEqual(left.members, right.members)
}

func pitMembershipDetails(snapshot pitRaftMembershipSnapshot) Details {
	members := make([]Details, 0, len(snapshot.members))
	for _, member := range snapshot.members {
		members = append(members, Details{
			"node_id":         member.id,
			"suffrage":        member.suffrage,
			"service_address": member.serviceAddress,
		})
	}

	return Details{
		"leader_id": snapshot.leader,
		"members":   members,
	}
}

func resolvePITConvergenceMembers(
	snapshot pitRaftMembershipSnapshot,
	conns PerNodeConns,
) ([]pitConvergenceMemberConn, bool) {
	byAddress := make(map[string]*PerNodeConn, len(conns))
	for _, conn := range conns {
		if conn == nil || conn.Addr == "" {
			continue
		}
		if _, exists := byAddress[conn.Addr]; exists {
			return nil, false
		}
		byAddress[conn.Addr] = conn
	}

	resolved := make([]pitConvergenceMemberConn, 0, len(snapshot.members))
	for _, member := range snapshot.members {
		conn := byAddress[member.serviceAddress]
		if conn == nil {
			return nil, false
		}
		resolved = append(resolved, pitConvergenceMemberConn{member: member, conn: conn})
	}

	return resolved, len(resolved) > 0
}

func waitPITConvergenceMembersAtExactIndex(
	ctx context.Context,
	members []pitConvergenceMemberConn,
	target uint64,
) (bool, map[string]any) {
	phaseCtx, cancel := context.WithTimeout(ctx, pitConvergenceIndexTimeout)
	defer cancel()

	ready := make(map[uint32]bool, len(members))
	last := make(map[string]any, len(members))
	for phaseCtx.Err() == nil {
		for _, member := range members {
			if ready[member.member.id] {
				continue
			}

			callCtx, cancelCall := context.WithTimeout(phaseCtx, pitConvergenceRPCTimeout)
			state, err := member.conn.Cluster.GetClusterState(
				callCtx,
				&clusterpb.GetClusterStateRequest{NodeId: member.member.id},
			)
			cancelCall()
			key := fmt.Sprintf("%d", member.member.id)
			if err != nil {
				last[key] = fmt.Sprintf("error: %v", err)

				continue
			}

			status := state.GetSyncProgress().GetStatus()
			persisted := state.GetRaftStatus().GetLastPersistedIndex()
			last[key] = Details{
				"status":               status,
				"last_persisted_index": persisted,
				"target":               target,
				"local_node":           state.GetLocalNode(),
				"suffrage":             member.member.suffrage,
			}
			if persisted > target {
				return false, last
			}
			if state.GetLocalNode() == member.member.id && status == "normal" && persisted == target {
				ready[member.member.id] = true
			}
		}

		if len(ready) == len(members) {
			return true, last
		}
		if !waitForPITConvergenceDelay(phaseCtx, pitConvergencePollInterval) {
			break
		}
	}

	return false, last
}

func observeEveryPITConvergenceMember(
	ctx context.Context,
	members []pitConvergenceMemberConn,
	ledgerID uint32,
	markerLogSequence uint64,
) (bool, map[string]any) {
	observations := make(map[string]any, len(members))
	var reference []CanonicalVolume
	for _, member := range members {
		callCtx, cancel := context.WithTimeout(ctx, pitConvergenceRPCTimeout)
		canonical, view, err := ObservePITConvergenceFixture(
			callCtx,
			member.conn.Bucket,
			ledgerID,
			markerLogSequence,
		)
		cancel()
		key := fmt.Sprintf("%d", member.member.id)
		if err != nil {
			observations[key] = fmt.Sprintf("error: %v", err)

			return false, observations
		}
		if reference == nil {
			reference = canonical
		} else if !reflect.DeepEqual(reference, canonical) {
			observations[key] = Details{
				"error":     "canonical aggregate differs from prior member",
				"reference": reference,
				"canonical": canonical,
			}

			return false, observations
		}

		observations[key] = Details{
			"service_address":  member.member.serviceAddress,
			"suffrage":         member.member.suffrage,
			"log_watermark":    view.GetLogWatermark(),
			"audit_watermark":  view.GetAuditWatermark(),
			"manifest_version": view.GetManifestVersion(),
			"canonical":        canonical,
		}
	}

	return len(observations) == len(members), observations
}

func waitForPITConvergenceRetry(ctx context.Context) bool {
	return waitForPITConvergenceDelay(ctx, pitConvergencePollInterval)
}

func waitForPITConvergenceDelay(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
