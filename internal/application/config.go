package application

import (
	"time"

	"github.com/formancehq/ledger-v3-poc/internal/service/node"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
)

type HealthConfig struct {
	Interval      time.Duration
	WALThreshold  float64
	DataThreshold float64
}

type Config struct {
	RaftConfig      node.NodeConfig
	Debug           bool
	HTTPPort        int
	GRPCPort        int
	TransportConfig node.TransportConfig
	DataDir         string
	PebbleConfig    data.Config
	HealthConfig    HealthConfig
	ClusterID       string
}

func (c Config) Validate() error {
	return c.RaftConfig.Validate()
}
