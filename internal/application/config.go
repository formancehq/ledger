package application

import (
	"github.com/formancehq/ledger-v3-poc/internal/raft"
)

type Config struct {
	RaftConfig      raft.NodeConfig
	Debug           bool
	HTTPPort        int
	TransportConfig raft.TransportConfig
	StorageType     string
	DataDir         string
}

func (c Config) Validate() error {
	return c.RaftConfig.Validate()
}
