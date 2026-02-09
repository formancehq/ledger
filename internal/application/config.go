package application

import (
	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

type Config struct {
	RaftConfig      node.NodeConfig
	Debug           bool
	HTTPPort        int
	GRPCPort        int
	TransportConfig node.TransportConfig
	DataDir         string
	PebbleConfig    data.Config
}

func (c Config) Validate() error {
	return c.RaftConfig.Validate()
}
