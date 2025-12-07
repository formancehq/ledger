package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	NodeID            uint64 // Numeric node ID
	BindAddr          string
	AdvertiseAddr     string
	DataDir           string
	Peers             []string          // Format: "<id>/<address>" (e.g., "1/node-1:8888")
	PeerIDs           map[string]uint64 // Map of peer address -> numeric ID (parsed from Peers)
	Debug             bool
	Bootstrap         bool
	GRPCPort          int
	HTTPPort          int
	StorageType       string // "sqlite" or "file"
	SQLiteDSN         string
	StorageFilePath   string        // Path to log file when using "file" storage type
	SnapshotThreshold uint64        // Number of logs before triggering a snapshot
	SnapshotInterval  time.Duration // Minimum interval between snapshots
}

// ParsePeers parses the peer list and populates PeerIDs map
// Format: "<id>/<address>" (e.g., "1/node-1:8888")
func (c *Config) ParsePeers() error {
	c.PeerIDs = make(map[string]uint64)
	for _, peer := range c.Peers {
		parts := strings.SplitN(peer, "/", 2)
		if len(parts) != 2 {
			return fmt.Errorf("invalid peer format: %s (expected format: <id>/<address>)", peer)
		}
		idStr := parts[0]
		address := parts[1]

		id, err := strconv.ParseUint(idStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid peer ID in %s: %w", peer, err)
		}
		if id == 0 {
			return fmt.Errorf("peer ID cannot be zero in %s", peer)
		}

		c.PeerIDs[address] = id
	}
	return nil
}

func (c *Config) Validate() error {
	if c.NodeID == 0 {
		return fmt.Errorf("node-id is required and must be non-zero")
	}

	// Node IDs must be < 0x10000 (65536) to avoid collision with bucket Raft groups
	// Bucket Raft groups use IDs >= 0x10000 (bucketID << 16)
	if c.NodeID >= 0x10000 {
		return fmt.Errorf("node-id must be < 0x10000 (65536), got %d (0x%x). Bucket Raft groups use IDs >= 0x10000", c.NodeID, c.NodeID)
	}

	// Parse peers if not already parsed
	if c.PeerIDs == nil {
		if err := c.ParsePeers(); err != nil {
			return fmt.Errorf("parsing peers: %w", err)
		}
	}

	// Validate peer IDs are also < 0x10000
	for addr, peerID := range c.PeerIDs {
		if peerID >= 0x10000 {
			return fmt.Errorf("peer ID for %s must be < 0x10000 (65536), got %d (0x%x)", addr, peerID, peerID)
		}
	}

	// If AdvertiseAddr is not set, use BindAddr
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.BindAddr
	}
	// Validate storage type
	if c.StorageType == "" {
		c.StorageType = "sqlite" // Default to sqlite
	}
	if c.StorageType != "sqlite" && c.StorageType != "file" {
		return fmt.Errorf("invalid storage type: %s (must be 'sqlite' or 'file')", c.StorageType)
	}
	if c.StorageType == "file" && c.StorageFilePath == "" {
		return fmt.Errorf("storage-file-path is required when storage-type is 'file'")
	}
	if c.StorageType == "sqlite" && c.SQLiteDSN == "" {
		return fmt.Errorf("sqlite-dsn is required when storage-type is 'sqlite'")
	}
	return nil
}
