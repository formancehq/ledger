package config

import (
	"fmt"
	"time"
)

type Config struct {
	NodeID            string
	BindAddr          string
	AdvertiseAddr     string
	DataDir           string
	Peers             []string
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

func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node-id is required")
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
