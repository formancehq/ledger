package grpc

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/application/membership"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/proto/clusterbootstrappb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// ClusterBootstrapServiceServerImpl serves the inter-node bootstrap surface
// on the RaftServer. It exposes only the two RPCs needed by a joining node
// (peer discovery + learner registration), gated by the RaftServer's own
// auth (cluster-id metadata + cluster-secret bearer when configured).
//
// No user JWT or scope check applies here; that is intentional — the
// joining node has no user identity to present at bootstrap time.
//
// Business invariants for membership changes live in the application
// membership.Service; this adapter only handles transport-specific
// concerns (cluster-id check, leader forwarding via the Raft transport,
// proto conversion, error-to-status mapping).
type ClusterBootstrapServiceServerImpl struct {
	clusterbootstrappb.UnimplementedClusterBootstrapServiceServer

	node                  *node.Node
	raftTransport         *node.DefaultTransport
	membership            *membership.Service
	logger                logging.Logger
	clusterID             string
	fsmDeterminismEnabled bool
}

func NewClusterBootstrapServiceServer(
	n *node.Node,
	raftTransport *node.DefaultTransport,
	membershipSvc *membership.Service,
	logger logging.Logger,
	clusterID string,
	fsmDeterminismEnabled bool,
) clusterbootstrappb.ClusterBootstrapServiceServer {
	return &ClusterBootstrapServiceServerImpl{
		node:                  n,
		raftTransport:         raftTransport,
		membership:            membershipSvc,
		logger:                logger.WithField("component", "cluster-bootstrap-server"),
		clusterID:             clusterID,
		fsmDeterminismEnabled: fsmDeterminismEnabled,
	}
}

// checkClusterID rejects calls that do not carry the expected cluster-id
// metadata, mirroring the check enforced on the Raft streaming transport.
// When clusterID is empty (single-node/unconfigured deployments), the
// check is a no-op.
func (impl *ClusterBootstrapServiceServerImpl) checkClusterID(ctx context.Context) error {
	if impl.clusterID == "" {
		return nil
	}

	values := metadata.ValueFromIncomingContext(ctx, node.MetadataKeyClusterID)
	if len(values) == 0 || values[0] != impl.clusterID {
		return status.Errorf(codes.PermissionDenied, "invalid cluster ID")
	}

	return nil
}

// GetPeers returns the current set of voter nodes with their Raft and
// service addresses. Called by a joining node before it starts Raft, to
// populate its initial WAL snapshot.
//
// node.GetClusterState only populates the nodes slice when the local
// node is the Raft leader (see node.go: "if status.RaftState ==
// raft.StateLeader"). On a follower the slice is empty, so the joining
// node would receive a useless answer. Forward the call to the leader
// to guarantee a meaningful response.
func (impl *ClusterBootstrapServiceServerImpl) GetPeers(ctx context.Context, req *clusterbootstrappb.GetPeersRequest) (*clusterbootstrappb.GetPeersResponse, error) {
	if err := impl.checkClusterID(ctx); err != nil {
		return nil, err
	}

	if !impl.node.IsLeader() {
		conn, err := impl.leaderRaftConn()
		if err != nil {
			return nil, convertToGRPCError(err, impl.logger)
		}

		outCtx := ctx
		if impl.clusterID != "" {
			outCtx = metadata.AppendToOutgoingContext(ctx, node.MetadataKeyClusterID, impl.clusterID)
		}

		return clusterbootstrappb.NewClusterBootstrapServiceClient(conn).GetPeers(outCtx, req)
	}

	peers, err := impl.membership.ListPeers(ctx)
	if err != nil {
		return nil, convertToGRPCError(err, impl.logger)
	}

	out := make([]*clusterbootstrappb.PeerInfo, 0, len(peers))
	for _, p := range peers {
		out = append(out, &clusterbootstrappb.PeerInfo{
			Id:             p.ID,
			RaftAddress:    p.RaftAddress,
			ServiceAddress: p.ServiceAddress,
		})
	}

	return &clusterbootstrappb.GetPeersResponse{Peers: out}, nil
}

// JoinAsLearner registers the calling node as a learner on the Raft
// cluster. On a follower, the call is forwarded to the leader's
// RaftServer. On the leader, it consults the removed-member registry
// (EN-1045) first — a rejoin with a blacklisted (nodeID, instance_id)
// tuple is refused with FailedPrecondition — then delegates to the
// application membership service which wires the peer into the
// transport/pool and proposes the ConfChange.
func (impl *ClusterBootstrapServiceServerImpl) JoinAsLearner(ctx context.Context, req *clusterbootstrappb.JoinAsLearnerRequest) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
	if err := impl.checkClusterID(ctx); err != nil {
		return nil, err
	}

	impl.logger.WithFields(map[string]any{
		"requestedNodeID":      req.GetNodeId(),
		"requestedRaftAddress": req.GetRaftAddress(),
		"requestedServiceAddr": req.GetServiceAddress(),
		"hasInstanceID":        len(req.GetInstanceId()) > 0,
		"isLeader":             impl.node.IsLeader(),
		"localNodeID":          impl.node.GetNodeID(),
	}).Infof("JoinAsLearner: received request")

	if !impl.node.IsLeader() {
		conn, err := impl.leaderRaftConn()
		if err != nil {
			impl.logger.Infof("JoinAsLearner: not leader and leader unreachable, returning ErrNoLeader")

			// The RaftServer has no error-conversion interceptor;
			// map commonpb.ErrNoLeader to codes.Unavailable here so
			// tryAddLearner treats it as transient and tries the
			// next peer instead of failing fatally.
			return nil, convertToGRPCError(err, impl.logger)
		}

		impl.logger.Infof("JoinAsLearner: forwarding to leader")

		// Re-inject the cluster-id on the outgoing call so the leader's own
		// checkClusterID pass on this same RPC. The incoming context's
		// metadata is not propagated to outgoing calls by default.
		outCtx := ctx
		if impl.clusterID != "" {
			outCtx = metadata.AppendToOutgoingContext(ctx, node.MetadataKeyClusterID, impl.clusterID)
		}

		return clusterbootstrappb.NewClusterBootstrapServiceClient(conn).JoinAsLearner(outCtx, req)
	}

	// Cross-peer fsm-determinism-enabled consistency. The persisted-config
	// validation on each node only catches a flag flip across restarts of the
	// SAME node; it cannot see a peer that boots with a different flag. Enforce
	// the cluster-wide invariant here, at the one point every joining node must
	// pass through: the deterministic attribute encoding and the cross-node FSM
	// digest are only coherent when every peer runs the same setting, so a
	// divergent peer must be refused rather than admitted and left reporting
	// perpetual (false) digest divergence. A node built before this field
	// existed sends the zero value (false); if the leader runs with the flag ON
	// that mismatch is (correctly) surfaced here.
	if req.GetFsmDeterminismEnabled() != impl.fsmDeterminismEnabled {
		impl.logger.WithFields(map[string]any{
			"nodeID":     req.GetNodeId(),
			"joinerFlag": req.GetFsmDeterminismEnabled(),
			"leaderFlag": impl.fsmDeterminismEnabled,
		}).Errorf("JoinAsLearner: refusing peer with mismatched fsm-determinism-enabled")

		return nil, status.Errorf(codes.FailedPrecondition,
			"fsm-determinism-enabled mismatch: joining node %d has %t but the cluster runs with %t; "+
				"every peer must set --fsm-determinism-enabled identically",
			req.GetNodeId(), req.GetFsmDeterminismEnabled(), impl.fsmDeterminismEnabled)
	}

	// EN-1045: every peer must present its 16-byte instance_id — clients
	// acquire one at first boot via wal.EnsureInstanceID.
	if len(req.GetInstanceId()) != 16 {
		return nil, status.Errorf(codes.InvalidArgument, "instance_id must be 16 bytes, got %d", len(req.GetInstanceId()))
	}

	// Refuse a peer whose (nodeID, instance_id) has been blacklisted.
	// Errors reading the store surface as Internal so an operator sees
	// the anomaly and can investigate.
	removed, err := impl.membership.IsRemoved(req.GetNodeId(), req.GetInstanceId())
	if err != nil {
		impl.logger.WithFields(map[string]any{
			"error":  err,
			"nodeID": req.GetNodeId(),
		}).Errorf("JoinAsLearner: reading removed-member registry failed")

		return nil, status.Error(codes.Internal, "reading removed-member registry")
	}

	if removed {
		impl.logger.WithFields(map[string]any{
			"nodeID":     req.GetNodeId(),
			"instanceID": hex.EncodeToString(req.GetInstanceId()),
		}).Infof("JoinAsLearner: refusing blacklisted peer (EN-1045)")

		return nil, status.Errorf(codes.FailedPrecondition,
			"node %d (instance %x) was previously removed from this cluster; "+
				"if this is intentional, run: ledgerctl cluster forget-removed %d %x",
			req.GetNodeId(), req.GetInstanceId(),
			req.GetNodeId(), req.GetInstanceId())
	}

	if err := impl.membership.AddLearner(ctx, req.GetNodeId(), req.GetRaftAddress(), req.GetServiceAddress(), req.GetInstanceId()); err != nil {
		// EN-1436: a JoinAsLearner call reaches the leader only when the caller
		// has no CLUSTER_JOINED marker on its WAL (see bootstrap/module.go
		// tryAddLearner). If the leader's Progress already carries this
		// nodeID, one of two things happened:
		//
		//   1. Legitimate previous member whose WAL was reprovisioned (chaos,
		//      operator, GitOps drift). Local raftLog.lastIndex is 0.
		//   2. Half-populated WAL from a crash between "initial snapshot
		//      write" and "CLUSTER_JOINED marker write". Local raftLog may
		//      have some entries but the marker did not persist.
		//
		// The old code returned AlreadyExists (mapped from
		// ErrNodeAlreadyInCluster) and the client treated it as idempotent
		// success. That works for shape 2 with a lucky lastIndex, and
		// silently breaks shape 1: as soon as raft starts on the caller,
		// the leader's next MsgApp/heartbeat carries Commit=Progress.Match,
		// etcd-raft's commitTo guard fires ("tocommit out of range"), and
		// the pod CrashLoopBackOffs indefinitely with no clue for the
		// operator. We cannot distinguish the two shapes from server-side
		// data (Progress.Match alone doesn't say whether the caller's log
		// covers it).
		//
		// Rather than silently patch this by force-removing on the caller's
		// behalf — which would mask real operational events (why did the
		// WAL disappear? did GitOps drift the PVC?) — surface it as a
		// FailedPrecondition with the exact remediation command in the
		// message. Client-side, tryAddLearner treats this specific reason as
		// fatal-with-clear-message so the pod fails fast with an operator-
		// actionable log line instead of crash-looping on tocommit.
		//
		// EN-1436: the guard keys on ErrNodeStaleProgress, which AddLearner
		// returns whenever the leader's Progress.Match for this node is
		// non-zero — covering BOTH the identical-identity rejoin and the
		// fresh-identity (WAL-wiped) rejoin, where a new instance_id would
		// otherwise route through a benign ConfChangeUpdateNode refresh and
		// bypass the fail-fast entirely. A distinguishing ErrorInfo detail
		// (reason STALE_RAFT_PROGRESS) is attached so the client can tell
		// this apart from the removed-member blacklist rejection above,
		// which is also FailedPrecondition but needs `forget-removed`, not
		// `remove-node --force`.
		if errors.Is(err, node.ErrNodeStaleProgress) {
			st := status.New(codes.FailedPrecondition, fmt.Sprintf(
				"node %d is already in the leader's raft Progress but the caller has no CLUSTER_JOINED marker — "+
					"its local WAL cannot satisfy the leader's known match index. "+
					"Reset membership first: `ledgerctl cluster remove-node %d --force` on the leader, then restart this pod.",
				req.GetNodeId(), req.GetNodeId()))

			detailed, detailErr := st.WithDetails(&errdetails.ErrorInfo{
				Reason: node.StaleRaftProgressReason,
				Domain: "ledger",
			})
			if detailErr != nil {
				// Attaching the detail failed (marshal error): fall back to
				// the bare status so the caller still fails fast, just
				// without the machine-readable reason.
				return nil, st.Err()
			}

			return nil, detailed.Err()
		}

		// Notably:
		//   - ErrNotLeader / ErrProposalDropped / ErrNoLeader → Unavailable
		//     (retried by the client)
		return nil, convertToGRPCError(err, impl.logger)
	}

	return &clusterbootstrappb.JoinAsLearnerResponse{}, nil
}

// leaderRaftConn returns a gRPC connection to the current leader's
// RaftServer, or commonpb.ErrNoLeader if the leader is unknown or
// unreachable through the Raft transport.
func (impl *ClusterBootstrapServiceServerImpl) leaderRaftConn() (*ggrpc.ClientConn, error) {
	leaderID := impl.node.GetLeader()
	if leaderID == 0 {
		return nil, commonpb.ErrNoLeader
	}

	conn := impl.raftTransport.GetPeerConnection(leaderID)
	if conn == nil {
		return nil, commonpb.ErrNoLeader
	}

	return conn, nil
}

func RegisterClusterBootstrapService(registrar ggrpc.ServiceRegistrar, server clusterbootstrappb.ClusterBootstrapServiceServer) {
	clusterbootstrappb.RegisterClusterBootstrapServiceServer(registrar, server)
}
