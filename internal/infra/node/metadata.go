package node

// gRPC metadata keys exchanged on the inter-node RaftServer surface.
// They identify the calling node and gate access by cluster membership;
// any RPC served on the RaftServer (Raft transport streams, snapshot
// service, ClusterBootstrapService) should rely on these constants
// rather than duplicating string literals.
const (
	MetadataKeyNodeID    = "nodeID"
	MetadataKeyClusterID = "clusterID"
	MetadataKeyPriority  = "priority"
	// MetadataKeyFSMDeterminism carries the connecting node's
	// fsm-determinism-enabled flag ("true"/"false") on every Raft transport
	// stream. The server rejects a stream whose peer disagrees with the local
	// flag: the deterministic attribute encoding and the cross-node FSM digest
	// are only coherent when every peer runs the same setting. This is the
	// enforcement point that covers the STATIC BOOTSTRAP path (seed nodes never
	// call JoinAsLearner, but they all establish Raft streams to each other),
	// complementing the join-time check in ClusterBootstrapService.
	MetadataKeyFSMDeterminism = "fsmDeterminism"
)
