package worker

import (
	"github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/storage"
	"go.uber.org/fx"
)

type ModuleConfig struct {
	AsyncBlockRunnerConfig storage.AsyncBlockRunnerConfig
	ReplicationConfig runner.ModuleConfig
}

func NewFXModule(cfg ModuleConfig) fx.Option {
	return fx.Options(
		storage.NewAsyncBlockRunnerModule(cfg.AsyncBlockRunnerConfig),
		runner.NewFXModule(cfg.ReplicationConfig),
	)
}
