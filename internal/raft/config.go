package raft

import (
	"time"
)

type NodeConfig struct {
	NodeID        uint64 // Numeric node ID
	Bootstrap     bool
	Peers         []Peer          // Format: "<id>/<address>" (e.g., "1/node-1:8888")
	DataDir       string
	SnapshotThreshold uint64        // Number of logs before triggering a snapshot
	SnapshotInterval  time.Duration // Minimum interval between snapshots
	AdvertiseAddr string
}

type Peer struct {
	ID uint64
	Address string
}