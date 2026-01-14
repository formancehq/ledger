package raft

import (
	"fmt"
	"time"
)

type NodeConfig struct {
	NodeID               uint64 // Numeric rawNode ID
	Peers                []Peer // Format: "<id>/<address>" (e.g., "1/rawNode-1:8888")
	WalDir               string
	SnapshotThreshold    uint64        // Number of logs before triggering a snapshot
	SnapshotInterval     time.Duration // Minimum interval between snapshots
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

func (c *NodeConfig) Validate() error {
	if c.NodeID == 0 {
		return fmt.Errorf("node-id is required and must be non-zero")
	}

	// Node IDs must be < 0x10000 (65536) to avoid collision with bucket Raft groups
	// Bucket Raft groups use IDs >= 0x10000 (bucketID << 16)
	if c.NodeID >= 0x10000 {
		return fmt.Errorf("node-id must be < 0x10000 (65536), got %d (0x%x). Bucket Raft groups use IDs >= 0x10000", c.NodeID, c.NodeID)
	}

	// Validate peer IDs are also < 0x10000
	for _, peer := range c.Peers {
		if peer.ID >= 0x10000 {
			return fmt.Errorf("peer ID for %s must be < 0x10000 (65536), got %d (0x%x)", peer.Address, peer.ID, peer.ID)
		}
	}

	// If AdvertiseAddr is not set, use BindAddr
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.BindAddr
	}

	return nil
}

type Peer struct {
	ID      uint64
	Address string
}
