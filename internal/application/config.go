package application

import (
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/store/pebble"
)

type Config struct {
	RaftConfig      raft.NodeConfig
	Debug           bool
	HTTPPort        int
	TransportConfig raft.TransportConfig
	StorageType     string
	DataDir         string
	PebbleConfig    pebble.Config
}

func (c Config) Validate() error {
	return c.RaftConfig.Validate()
}
