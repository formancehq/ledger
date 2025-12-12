package raft

import (
	"time"
)

type NodeConfig struct {
	NodeID            uint64 // Numeric node ID
	Bootstrap         bool
	Peers             []Peer // Format: "<id>/<address>" (e.g., "1/node-1:8888")
	DataDir           string
	SnapshotThreshold uint64        // Number of logs before triggering a snapshot
	SnapshotInterval  time.Duration // Minimum interval between snapshots
	ElectionTick      int    // Election timeout in ticks (default: 10)
	HeartbeatTick     int    // Heartbeat interval in ticks (default: 1)
	MaxSizePerMsg     uint64 // Maximum size per message in bytes (default: 1MB)
	MaxInflightMsgs   int    // Maximum number of in-flight messages (default: 256)
	TickInterval      time.Duration
}

type Peer struct {
	ID      uint64
	Address string
}
