package application

import (
	"github.com/formancehq/ledger-v3-poc/internal/raft/system"
)

type Config struct {
	RaftConfig system.Config
	Debug      bool
	HTTPPort   int
}

func (c Config) Validate() error {
	return c.RaftConfig.Validate()
}
