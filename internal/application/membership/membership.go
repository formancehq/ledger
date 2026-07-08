// Package membership owns the invariants of Raft cluster membership
// changes (learner registration, voter promotion, node removal) and the
// composite peer view used at bootstrap.
//
// Two gRPC surfaces depend on these operations:
//
//   - ClusterService on the external ServiceServer, gated by user JWT
//     scopes (ledger:ClusterWrite, ledger:ClusterRead), used by ledgerctl and
//     observability tooling.
//   - ClusterBootstrapService on the inter-node RaftServer, gated by
//     cluster-id metadata (+ cluster-secret bearer when configured),
//     used by a joining node before it has any user identity.
//
// Both adapters share the same underlying state machine: validate the
// request → mutate the local transport pools so the leader can reach
// the peer → propose the matching ConfChange. Keeping that sequence in
// a single type prevents the two adapters from drifting (e.g. one
// forgetting to add the peer to the service pool, or one ignoring a
// future per-operation invariant such as "no membership change while
// in maintenance mode").
//
// The adapter layer is still responsible for:
//   - authentication (different on each surface),
//   - leader forwarding (different RPC clients on each surface),
//   - error-to-status mapping (different per transport).
//
// This package strictly handles the leader-local mutation. Forwarding
// from a follower never reaches it.
package membership

import (
	"context"
	"errors"
	"fmt"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/membership"
	"github.com/formancehq/ledger/v3/internal/infra/node"
	"github.com/formancehq/ledger/v3/internal/infra/transport"
)

// Peer is the minimal view of a cluster member as returned by ListPeers.
// It is intentionally proto-free so the application layer does not leak
// gRPC types up the dependency graph; adapters convert to their own
// proto on the way out.
type Peer struct {
	ID             uint64
	RaftAddress    string
	ServiceAddress string
}

// Service is the single owner of cluster membership operations.
type Service struct {
	node             *node.Node
	raftTransport    *node.DefaultTransport
	servicePool      *transport.ConnectionPool
	infraMembership  *membership.Membership
	logger           logging.Logger
	localRaftAddr    string
	localServiceAddr string
}

func NewService(
	n *node.Node,
	raftTransport *node.DefaultTransport,
	servicePool *transport.ConnectionPool,
	infraMembership *membership.Membership,
	logger logging.Logger,
	localRaftAddr, localServiceAddr string,
) *Service {
	return &Service{
		node:             n,
		raftTransport:    raftTransport,
		servicePool:      servicePool,
		infraMembership:  infraMembership,
		logger:           logger.WithField("component", "cluster-membership"),
		localRaftAddr:    localRaftAddr,
		localServiceAddr: localServiceAddr,
	}
}

// IsRemoved reports whether the (nodeID, instanceID) tuple is blacklisted
// (EN-1045). Passthrough to the infra Membership; kept on the Service so
// gRPC adapters depend only on this application-layer surface.
func (s *Service) IsRemoved(nodeID uint64, instanceID []byte) (bool, error) {
	return s.infraMembership.IsRemoved(nodeID, instanceID)
}

// AddLearner wires the new peer into the local transport pools and
// proposes the AddLearner ConfChange on the leader. The caller must
// have already routed the request to the leader. instanceID is the
// joining peer's 16-byte identity UUID (EN-1045); may be empty for
// legacy clients that predate the field.
//
// Returns node.ErrNodeAlreadyInCluster on idempotent retries; adapters
// map that to their transport-specific "already-exists" status.
func (s *Service) AddLearner(ctx context.Context, nodeID uint64, raftAddr, serviceAddr string, instanceID []byte) error {
	if err := validatePeer(nodeID, raftAddr, serviceAddr); err != nil {
		return err
	}

	s.logger.WithFields(map[string]any{
		"learnerNodeID":  nodeID,
		"raftAddress":    raftAddr,
		"serviceAddress": serviceAddr,
	}).Infof("AddLearner: wiring peer and proposing ConfChange")

	s.raftTransport.AddPeer(nodeID, raftAddr)

	if err := s.servicePool.AddPeer(nodeID, serviceAddr); err != nil {
		// Non-fatal: the leader can still propose the ConfChange; the
		// service pool will be refreshed by the ConfChange observer
		// after commit. Surface as a warning so it stays visible.
		s.logger.WithFields(map[string]any{"error": err}).Errorf("AddLearner: failed to add learner to service pool")
	}

	if err := s.node.AddLearner(ctx, nodeID, raftAddr, serviceAddr, instanceID); err != nil {
		return fmt.Errorf("adding learner: %w", err)
	}

	return nil
}

// PromoteLearner proposes the promote-to-voter ConfChange. Caller must
// have routed to the leader.
func (s *Service) PromoteLearner(ctx context.Context, nodeID uint64) error {
	if nodeID == 0 {
		return errors.New("node_id must be non-zero")
	}

	if err := s.node.PromoteLearner(ctx, nodeID); err != nil {
		return fmt.Errorf("promoting learner: %w", err)
	}

	return nil
}

// RemoveNode proposes the removal of a node from the cluster. When
// force is true the removal bypasses Raft consensus and must be issued
// directly on the leader (used by the operator when a pod is gone and
// the consensus path is stuck).
func (s *Service) RemoveNode(ctx context.Context, nodeID uint64, force bool) error {
	if nodeID == 0 {
		return errors.New("node_id must be non-zero")
	}

	if force {
		if err := s.node.ForceRemoveNode(ctx, nodeID); err != nil {
			return fmt.Errorf("force-removing node: %w", err)
		}

		return nil
	}

	if err := s.node.RemoveNode(ctx, nodeID); err != nil {
		return fmt.Errorf("removing node: %w", err)
	}

	return nil
}

// ListPeers returns the current cluster members enriched with their
// Raft and service addresses. The local node fills in its own
// addresses from the constructor; remote addresses come from the
// transport / service pools populated by the ConfChange observer.
func (s *Service) ListPeers(ctx context.Context) ([]Peer, error) {
	clusterState, err := s.node.GetClusterState(ctx)
	if err != nil {
		return nil, fmt.Errorf("reading cluster state: %w", err)
	}

	localNodeID := s.node.GetNodeID()
	peers := make([]Peer, 0, len(clusterState.GetNodes()))

	for _, n := range clusterState.GetNodes() {
		nodeID := uint64(n.GetId())

		var raftAddr, serviceAddr string
		if nodeID == localNodeID {
			raftAddr = s.localRaftAddr
			serviceAddr = s.localServiceAddr
		} else {
			raftAddr = s.raftTransport.GetPeerAddress(nodeID)
			serviceAddr = s.servicePool.GetPeerAddress(nodeID)
		}

		if raftAddr == "" || serviceAddr == "" {
			continue
		}

		peers = append(peers, Peer{
			ID:             nodeID,
			RaftAddress:    raftAddr,
			ServiceAddress: serviceAddr,
		})
	}

	return peers, nil
}

func validatePeer(nodeID uint64, raftAddr, serviceAddr string) error {
	if nodeID == 0 {
		return errors.New("node_id must be non-zero")
	}

	if raftAddr == "" {
		return errors.New("raft_address is required")
	}

	if serviceAddr == "" {
		return errors.New("service_address is required")
	}

	return nil
}
