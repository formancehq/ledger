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
	NextLedgerID uint64                // Next sequential ledger ID
	Ledgers      map[string]LedgerInfo // Map of ledger name -> ledger info
}

// LedgerState represents the state of the ledger FSM
type LedgerState struct {
	LedgerInfo   LedgerInfo // Ledger information
	LastSequence uint64     // Last global sequence number
}
