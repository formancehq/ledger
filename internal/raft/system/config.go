package system

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/raft"
)

type Config struct {
	raft.NodeConfig
	AdvertiseAddr string
	BindAddr      string
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
