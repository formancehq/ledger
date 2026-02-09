package node

import (
	"fmt"
	"time"
)

type NodeConfig struct {
	NodeID               uint64 // Numeric rawNode ID
	Peers                []Peer // Format: "<id>/<raftAddress>/<serviceAddress>" (e.g., "1/node-1:7777/node-1:8888")
	WalDir               string
	SnapshotThreshold    uint64        // Number of logs before triggering a snapshot
	SnapshotInterval     time.Duration // Minimum interval between snapshots
	RotationThreshold    uint64        // Number of entries before rotating generations (default: 1000)
	ElectionTick         int           // Election timeout in ticks (default: 10)
	HeartbeatTick        int           // Heartbeat interval in ticks (default: 1)
	MaxSizePerMsg        uint64        // Maximum size per message in bytes (default: 1MB)
	MaxInflightMsgs      int           // Maximum number of in-flight messages (default: 256)
	TickInterval         time.Duration
	CompactionMargin     uint64 // Compaction margin in number of logs
	ProposeQueueCapacity int    // Capacity of the propose queue (default: 100)
	AdvertiseAddr        string
	BindAddr             string
}

func (cfg *NodeConfig) Validate() error {
	if cfg.NodeID == 0 {
		return fmt.Errorf("node-id is required and must be non-zero")
	}

	// If AdvertiseAddr is not set, use BindAddr
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = cfg.BindAddr
	}

	return nil
}

func (cfg *NodeConfig) SetDefaults() {
	if cfg.ElectionTick == 0 {
		cfg.ElectionTick = 10
	}
	if cfg.HeartbeatTick == 0 {
		cfg.HeartbeatTick = 1
	}
	if cfg.MaxSizePerMsg == 0 {
		cfg.MaxSizePerMsg = 1024 * 1024 // 1MB
	}
	if cfg.MaxInflightMsgs == 0 {
		cfg.MaxInflightMsgs = 256
	}
	if cfg.SnapshotThreshold == 0 {
		cfg.SnapshotThreshold = 1000
	}
	if cfg.RotationThreshold == 0 {
		cfg.RotationThreshold = 1000
	}
	if cfg.ProposeQueueCapacity == 0 {
		cfg.ProposeQueueCapacity = 100
	}
}

type Peer struct {
	ID             uint64
	Address        string // Raft transport address
	ServiceAddress string // Service API address (for request forwarding)
}
