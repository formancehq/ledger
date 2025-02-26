package cmd

import (
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

type workerConfiguration struct {
	hashLogsBlockMaxSize   int
	hashLogsBlockCRONSpec  string
}

func discoverWorkerConfiguration(cmd *cobra.Command) workerConfiguration {
	ret := workerConfiguration{}
	ret.hashLogsBlockCRONSpec, _ = cmd.Flags().GetString(WorkerAsyncBlockHasherScheduleFlag)
	ret.hashLogsBlockMaxSize, _ = cmd.Flags().GetInt(WorkerAsyncBlockHasherMaxBlockSizeFlag)

	return ret
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

			workerConfiguration := discoverWorkerConfiguration(cmd)

			return service.New(cmd.OutOrStdout(),
				fx.NopLogger,
				otlp.FXModuleFromFlags(cmd),
				otlptraces.FXModuleFromFlags(cmd),
				otlpmetrics.FXModuleFromFlags(cmd),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
				storage.NewFXModule(storage.ModuleConfig{
					// todo
				}),
				worker.NewFXModule(worker.ModuleConfig{
					MaxBlockSize: workerConfiguration.hashLogsBlockMaxSize,
					Schedule: workerConfiguration.hashLogsBlockCRONSpec,
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
