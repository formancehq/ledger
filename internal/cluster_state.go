package ledger

import (
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
)

// RaftStatus represents the complete Raft status information
type RaftStatus struct {
	// Current Raft state (Leader, Follower, Candidate, PreCandidate)
	State string `json:"state"`
	// Current term
	Term uint64 `json:"term"`
	// ID of the current leader (0 if no leader)
	Leader uint64 `json:"leader"`
	// Index of the last applied entry
	Applied uint64 `json:"applied"`
	// Index of the last committed entry
	Commit uint64 `json:"commit"`
	// Index of the last entry in the log
	LastIndex uint64 `json:"lastIndex"`
	// ID of the node that received the vote (0 if no vote)
	Vote uint64 `json:"vote"`
	// Progress information for each node in the cluster
	Progress map[uint64]ProgressInfo `json:"progress"`
}

// ProgressInfo represents the progress information for a node
type ProgressInfo struct {
	// Match index: highest log entry known to be replicated to this node
	Match uint64 `json:"match"`
	// Next index: index of the next log entry to send to this node
	Next uint64 `json:"next"`
	// State: Probe, Replicate, or Snapshot
	State string `json:"state"`
	// PendingSnapshot: index of pending snapshot (0 if none)
	PendingSnapshot uint64 `json:"pendingSnapshot"`
	// Whether this node is recently active
	RecentActive bool `json:"recentActive"`
	// Whether probe message was sent (paused)
	ProbeSent bool `json:"probeSent"`
	// Whether this node is paused (IsPaused() method result)
	IsPaused bool `json:"isPaused"`
}

// ClusterState represents the state of the Raft cluster
type ClusterState[InnerState any] struct {
	State      string      `json:"state"`      // Leader, Follower, Candidate, Shutdown
	Leader     uint        `json:"leader"`     // ID of the current leader (0 if no leader)
	Nodes      []NodeInfo  `json:"nodes"`      // List of all nodes in the cluster
	LocalNode  uint        `json:"localNode"`  // ID of the local node
	RaftStatus *RaftStatus `json:"raftStatus"` // Complete Raft status information
	InnerState InnerState  `json:"innerState"`
}

// NodeInfo represents information about a node in the cluster
type NodeInfo struct {
	ID       uint   `json:"id"`       // Node ID
	Address  string `json:"address"`  // Node address
	Suffrage string `json:"suffrage"` // Voter or Nonvoter
}

type SystemState struct {
	NextLedgerID uint64                          // Next sequential ledger ID
	Ledgers      map[string]*ledgerpb.LedgerInfo // Map of ledger name -> ledger info
}

// LedgerState represents the state of the ledger FSM
type LedgerState struct {
	LedgerInfo *ledgerpb.LedgerInfo `json:"ledgerInfo"`
	LastLogID  uint64               `json:"lastLogID"`
}
