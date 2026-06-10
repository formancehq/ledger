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
)
