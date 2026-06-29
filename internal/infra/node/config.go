package node

import (
	"errors"
	"fmt"
	"time"
)

// Upper bounds on validated numeric fields. They are intentionally generous —
// the goal is not to enforce policy but to fail fast on values that are
// obviously wrong (negative, or so large the allocation crashes the node)
// before they reach the channel constructor or errgroup.SetLimit at startup.
const (
	// maxQueueCapacity caps every Raft-related channel buffer size. 1M
	// pending entries is several orders of magnitude beyond steady-state
	// throughput; anything larger almost always reflects a misconfigured
	// flag rather than intent.
	maxQueueCapacity = 1 << 20

	// maxBufferBytes caps per-peer / per-message byte sizes. 1 GiB is the
	// outer envelope for a single Raft message; beyond it, allocating the
	// buffer is itself a denial-of-service.
	maxBufferBytes = 1 << 30

	// maxRaftTicks caps tick counts. Raft tick semantics break down well
	// before this, but it's enough to filter typos like "100000".
	maxRaftTicks = 10000
)

type NodeConfig struct {
	NodeID              uint64 // Numeric rawNode ID
	Peers               []Peer // Format: "<id>/<raftAddress>/<serviceAddress>" (e.g., "1/node-1:7777/node-1:8888")
	WalDir              string
	DataDir             string        // Data directory (for detecting RESTORED marker)
	MaintenanceInterval time.Duration // Interval for background WAL snapshot + Pebble checkpoint (default: 30s)

	RotationThreshold    uint64 // Number of entries before rotating generations (default: 1000)
	ElectionTick         int    // Election timeout in ticks (default: 10)
	HeartbeatTick        int    // Heartbeat interval in ticks (default: 1)
	MaxSizePerMsg        uint64 // Maximum size per message in bytes (default: 1MB)
	MaxInflightMsgs      int    // Maximum number of in-flight messages (default: 256)
	TickInterval         time.Duration
	CompactionMargin     uint64 // Compaction margin in number of logs
	ProposeQueueCapacity int    // Capacity of the propose queue (default: 100)
	AdvertiseAddr        string
	BindAddr             string
	// ServiceAdvertiseAddr is the address this node advertises for its
	// service API (gRPC client-facing). It is persisted alongside
	// AdvertiseAddr in the peer store so the cluster knows how to forward
	// client requests after a restart, even when this node is the only
	// voter (the bootstrap voter) and no Raft ConfChange ever publishes
	// its address. EN-1413.
	ServiceAdvertiseAddr   string
	TransportBufferSize    int           // Per-peer send buffer capacity in bytes (default: 10MB)
	ProcessingTickInterval time.Duration // Interval for processing committed entries (default: TickInterval/10)
	ReplayBatchSize        int           // Number of entries per batch during spool replay (default: 1000)
	Bootstrap              bool          // When true, initialize a new single-node cluster (this node is the sole voter)
	AutoPromoteThreshold   uint64        // Number of log entries a learner may lag behind the commit index before auto-promotion (0 = disable)
}

func (cfg *NodeConfig) Validate() error {
	if cfg.NodeID == 0 {
		return errors.New("node-id is required and must be non-zero")
	}

	// If AdvertiseAddr is not set, use BindAddr
	if cfg.AdvertiseAddr == "" {
		cfg.AdvertiseAddr = cfg.BindAddr
	}

	// Numeric fields use 0 as "use the default" sentinel (see SetDefaults),
	// so accept 0 and only reject negative or absurd values. The Raft
	// channel constructors panic on negative capacities and OOM on huge
	// ones — catch both here.
	for _, c := range []struct {
		name string
		val  int
		max  int
	}{
		{"propose-queue-capacity", cfg.ProposeQueueCapacity, maxQueueCapacity},
		{"transport-buffer-size", cfg.TransportBufferSize, maxBufferBytes},
		{"replay-batch-size", cfg.ReplayBatchSize, maxQueueCapacity},
		{"election-tick", cfg.ElectionTick, maxRaftTicks},
		{"heartbeat-tick", cfg.HeartbeatTick, maxRaftTicks},
		{"max-inflight-msgs", cfg.MaxInflightMsgs, maxQueueCapacity},
	} {
		if c.val < 0 || c.val > c.max {
			return fmt.Errorf("--%s must be in [0, %d] (got %d)", c.name, c.max, c.val)
		}
	}

	// uint64 fields can't go negative; just enforce the upper bound.
	for _, c := range []struct {
		name string
		val  uint64
		max  uint64
	}{
		{"max-size-per-msg", cfg.MaxSizePerMsg, maxBufferBytes},
		{"rotation-threshold", cfg.RotationThreshold, maxQueueCapacity},
		{"compaction-margin", cfg.CompactionMargin, maxQueueCapacity},
		{"auto-promote-threshold", cfg.AutoPromoteThreshold, maxQueueCapacity},
	} {
		if c.val > c.max {
			return fmt.Errorf("--%s must be ≤ %d (got %d)", c.name, c.max, c.val)
		}
	}

	if cfg.MaintenanceInterval < 0 {
		return fmt.Errorf("--maintenance-interval must be ≥ 0 (got %s)", cfg.MaintenanceInterval)
	}

	if cfg.TickInterval < 0 {
		return fmt.Errorf("--tick-interval must be ≥ 0 (got %s)", cfg.TickInterval)
	}

	if cfg.ProcessingTickInterval < 0 {
		return fmt.Errorf("--processing-tick-interval must be ≥ 0 (got %s)", cfg.ProcessingTickInterval)
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

	if cfg.MaintenanceInterval == 0 {
		cfg.MaintenanceInterval = 30 * time.Second
	}

	if cfg.RotationThreshold == 0 {
		cfg.RotationThreshold = 1000
	}

	if cfg.ProposeQueueCapacity == 0 {
		cfg.ProposeQueueCapacity = 100
	}

	if cfg.TransportBufferSize == 0 {
		cfg.TransportBufferSize = 10 * 1024 * 1024 // 10MB
	}

	if cfg.ReplayBatchSize == 0 {
		cfg.ReplayBatchSize = 1000
	}
}

type Peer struct {
	ID             uint64
	Address        string // Raft transport address
	ServiceAddress string // Service API address (for request forwarding)
}
