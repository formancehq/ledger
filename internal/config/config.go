package config

import "fmt"

type Config struct {
	NodeID        string
	BindAddr      string
	AdvertiseAddr string
	DataDir       string
	Peers         []string
	Debug         bool
	Bootstrap     bool
	GRPCPort      int
	HTTPPort      int
	SQLiteDSN     string
}

func (c *Config) Validate() error {
	if c.NodeID == "" {
		return fmt.Errorf("node-id is required")
	}
	// If AdvertiseAddr is not set, use BindAddr
	if c.AdvertiseAddr == "" {
		c.AdvertiseAddr = c.BindAddr
	}
	return nil
}
