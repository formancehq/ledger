package application

import (
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

type Config struct {
	RaftConfig      raft.NodeConfig
	Debug           bool
	HTTPPort        int
	TransportConfig raft.TransportConfig
	DataDir         string
	PebbleConfig    store.Config
}

func (c Config) Validate() error {
	return c.RaftConfig.Validate()
}
