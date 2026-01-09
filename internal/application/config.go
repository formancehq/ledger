package application

import (
	"github.com/formancehq/ledger-v3-poc/internal/raft"
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
)

type Config struct {
	RaftConfig system.NodeConfig
	Debug      bool
	HTTPPort   int
	TransportConfig raft.TransportConfig
}

func (c Config) Validate() error {
	return c.RaftConfig.Validate()
}
