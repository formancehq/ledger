package health

// nodeState is the narrow slice of *node.Node that the health components depend
// on. It is declared here (consumer side) so the leadership-gated write-gate
// logic (HealthChecker) and the readiness computation (GRPCHealthUpdater) are
// unit-testable without standing up a real Raft node. *node.Node satisfies it.
//
//go:generate go tool mockgen -write_source_comment=false -write_package_comment=false -source node_state.go -destination node_state_generated_test.go -package health
type nodeState interface {
	// IsLeader reports whether this node is the current Raft leader.
	IsLeader() bool
	// GetNodeID returns this node's Raft ID.
	GetNodeID() uint64
	// IsHealthy reports whether this node is a connected cluster member
	// (leader or follower).
	IsHealthy() bool
	// GetLeader returns the current leader's node ID, or 0 if none is elected.
	GetLeader() uint64
}
