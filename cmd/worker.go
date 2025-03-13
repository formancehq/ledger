package cmd

import (
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/go-libs/v2/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v2/otlp/otlptraces"
	"github.com/formancehq/go-libs/v2/service"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/worker"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
)

const (
	WorkerAsyncBlockHasherMaxBlockSizeFlag = "worker-async-block-hasher-max-block-size"
	WorkerAsyncBlockHasherScheduleFlag     = "worker-async-block-hasher-schedule"
)

type WorkerConfiguration struct {
	HashLogsBlockMaxSize  int    `mapstructure:"worker-async-block-hasher-max-block-size"`
	HashLogsBlockCRONSpec string `mapstructure:"worker-async-block-hasher-schedule"`
}

func addWorkerFlags(cmd *cobra.Command) {
	cmd.Flags().Int(WorkerAsyncBlockHasherMaxBlockSizeFlag, 1000, "Max block size")
	cmd.Flags().String(WorkerAsyncBlockHasherScheduleFlag, "0 * * * * *", "Schedule")
}

func NewWorkerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "worker",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}

			cfg, err := LoadConfig[WorkerConfiguration](cmd)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			return service.New(cmd.OutOrStdout(),
				fx.NopLogger,
				otlp.FXModuleFromFlags(cmd),
				otlptraces.FXModuleFromFlags(cmd),
				otlpmetrics.FXModuleFromFlags(cmd),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
				storage.NewFXModule(storage.ModuleConfig{}),
				worker.NewFXModule(worker.ModuleConfig{
					MaxBlockSize: cfg.HashLogsBlockMaxSize,
					Schedule:     cfg.HashLogsBlockCRONSpec,
				}),
			).Run(cmd)
		},
	}

	addWorkerFlags(cmd)
	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())

	return cmd
}
