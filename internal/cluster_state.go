package ledger

// ClusterState represents the state of the Raft cluster
type ClusterState[InnerState any] struct {
	State      string     `json:"state"`     // Leader, Follower, Candidate, Shutdown
	Leader     uint       `json:"leader"`    // ID of the current leader (0 if no leader)
	Nodes      []NodeInfo `json:"nodes"`     // List of all nodes in the cluster
	LocalNode  uint       `json:"localNode"` // ID of the local node
	InnerState InnerState `json:"innerState"`
}

// NodeInfo represents information about a node in the cluster
type NodeInfo struct {
	ID       uint   `json:"id"`       // Node ID
	Address  string `json:"address"`  // Node address
	Suffrage string `json:"suffrage"` // Voter or Nonvoter
}

type SystemState struct {
	NextBucketID uint64                // Next sequential bucket ID
	Buckets      map[string]BucketInfo // Map of bucket name -> bucket node
}

// BucketState represents the state of the bucket FSM
type BucketState struct {
	Ledgers      map[string]LedgerInfo // Map of ledger name -> ledger info
	LastSequence uint64                // Last global sequence number
}
