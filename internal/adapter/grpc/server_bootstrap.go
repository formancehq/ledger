package grpc

import (
	"context"
	"errors"
	"fmt"

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

	node          *node.Node
	raftTransport *node.DefaultTransport
	membership    *membership.Service
	logger        logging.Logger
	clusterID     string
}

func NewClusterBootstrapServiceServer(
	n *node.Node,
	raftTransport *node.DefaultTransport,
	membershipSvc *membership.Service,
	logger logging.Logger,
	clusterID string,
) clusterbootstrappb.ClusterBootstrapServiceServer {
	return &ClusterBootstrapServiceServerImpl{
		node:          n,
		raftTransport: raftTransport,
		membership:    membershipSvc,
		logger:        logger.WithField("component", "cluster-bootstrap-server"),
		clusterID:     clusterID,
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
// RaftServer. On the leader, it delegates to the application membership
// service which wires the peer into the transport/pool and proposes the
// ConfChange.
func (impl *ClusterBootstrapServiceServerImpl) JoinAsLearner(ctx context.Context, req *clusterbootstrappb.JoinAsLearnerRequest) (*clusterbootstrappb.JoinAsLearnerResponse, error) {
	if err := impl.checkClusterID(ctx); err != nil {
		return nil, err
	}

	impl.logger.WithFields(map[string]any{
		"requestedNodeID":      req.GetNodeId(),
		"requestedRaftAddress": req.GetRaftAddress(),
		"requestedServiceAddr": req.GetServiceAddress(),
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

	if err := impl.membership.AddLearner(ctx, req.GetNodeId(), req.GetRaftAddress(), req.GetServiceAddress()); err != nil {
		// EN-1436: if the node is already in the leader's Progress, its Match is
		// stale w.r.t. the caller. JoinAsLearner is only invoked when the caller
		// has no CLUSTER_JOINED marker on its WAL (see bootstrap/module.go); its
		// local log is therefore either fresh (never joined) or half-populated
		// from a previous life. Treating "already exists" as a no-op — as the
		// old code did — leaves Progress[nodeID].Match pointing at state the
		// caller doesn't have. As soon as raft starts on the caller it receives
		// a MsgApp/heartbeat with Commit=Progress.Match, panics with
		// "tocommit out of range" (raftLog.lastIndex < commit), and
		// CrashLoopBackOffs indefinitely.
		//
		// Reset Progress by force-removing and re-adding. Both ConfChanges are
		// cheap. The result on the caller is a fresh Progress with Match=0,
		// State=Probe, which triggers a proper MsgSnap catch-up.
		//
		// The other cases fall through untouched:
		//   - ErrNotLeader / ErrProposalDropped / ErrNoLeader → Unavailable
		//     (retried by the client)
		//   - anything else → propagated as-is
		if errors.Is(err, node.ErrNodeAlreadyInCluster) {
			impl.logger.WithFields(map[string]any{
				"nodeID":         req.GetNodeId(),
				"raftAddress":    req.GetRaftAddress(),
				"serviceAddress": req.GetServiceAddress(),
			}).Infof("JoinAsLearner: node already in cluster with stale Progress; force-removing before re-adding")

			if rmErr := impl.membership.RemoveNode(ctx, req.GetNodeId(), true); rmErr != nil {
				return nil, convertToGRPCError(fmt.Errorf("force-removing stale membership for rejoin: %w", rmErr), impl.logger)
			}

			if addErr := impl.membership.AddLearner(ctx, req.GetNodeId(), req.GetRaftAddress(), req.GetServiceAddress()); addErr != nil {
				return nil, convertToGRPCError(fmt.Errorf("re-adding learner after stale-membership cleanup: %w", addErr), impl.logger)
			}

			return &clusterbootstrappb.JoinAsLearnerResponse{}, nil
		}

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
