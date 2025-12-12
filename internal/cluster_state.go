package ledger

// ClusterState represents the state of the Raft cluster
type ClusterState struct {
	State     string     `json:"state"`     // Leader, Follower, Candidate, Shutdown
	Leader    uint       `json:"leader"`    // ID of the current leader (0 if no leader)
	Nodes     []NodeInfo `json:"nodes"`     // List of all nodes in the cluster
	LocalNode uint       `json:"localNode"` // ID of the local node
}

// NodeInfo represents information about a node in the cluster
type NodeInfo struct {
	ID       uint   `json:"id"`       // Node ID
	Address  string `json:"address"`  // Node address
	Suffrage string `json:"suffrage"` // Voter or Nonvoter
}
