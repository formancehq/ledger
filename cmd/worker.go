package cmd

import (
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/go-libs/v2/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v2/otlp/otlptraces"
	"github.com/formancehq/go-libs/v2/service"
	replication "github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/drivers/all"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/worker"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"time"
)

const (
	WorkerPipelinesSyncPeriodFlag      = "worker-pipelines-sync-period"
	WorkerPipelinesPullIntervalFlag    = "worker-pipelines-pull-interval"
	WorkerPipelinesPushRetryPeriodFlag = "worker-pipelines-push-retry-period"

	WorkerAsyncBlockHasherMaxBlockSizeFlag = "worker-async-block-hasher-max-block-size"
	WorkerAsyncBlockHasherScheduleFlag     = "worker-async-block-hasher-schedule"
)

type workerConfiguration struct {
	AsyncBlockRunnerConfig storage.AsyncBlockRunnerConfig
	ReplicationConfig 	replication.ModuleConfig
}

func discoverWorkerConfiguration(cmd *cobra.Command) (*workerConfiguration, error) {
	ret := &workerConfiguration{}
	hashLogsBlockCRONSpec, _ := cmd.Flags().GetString(WorkerAsyncBlockHasherScheduleFlag)
	if hashLogsBlockCRONSpec == "" {
		hashLogsBlockCRONSpec = "0 * * * * *"
	}
	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow)

	var err error
	ret.AsyncBlockRunnerConfig.Schedule, err = parser.Parse(hashLogsBlockCRONSpec)
	if err != nil {
		return nil, err
	}

	ret.AsyncBlockRunnerConfig.MaxBlockSize, _ = cmd.Flags().GetInt(WorkerAsyncBlockHasherMaxBlockSizeFlag)
	ret.ReplicationConfig.SyncPeriod, _ = cmd.Flags().GetDuration(WorkerPipelinesSyncPeriodFlag)
	ret.ReplicationConfig.PullInterval, _ = cmd.Flags().GetDuration(WorkerPipelinesPullIntervalFlag)
	ret.ReplicationConfig.PushRetryPeriod, _ = cmd.Flags().GetDuration(WorkerPipelinesPushRetryPeriodFlag)

	return ret, nil
}

func addWorkerFlags(cmd *cobra.Command) {
	cmd.Flags().Duration(WorkerPipelinesSyncPeriodFlag, 5*time.Second, "Pipelines sync period")
	cmd.Flags().Duration(WorkerPipelinesPullIntervalFlag, replication.DefaultPullInterval, "Pipelines pull interval")
	cmd.Flags().Duration(WorkerPipelinesPushRetryPeriodFlag, replication.DefaultPushRetryPeriod, "Pipelines push retry period")
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

			workerConfiguration, err := discoverWorkerConfiguration(cmd)
			if err != nil {
				return err
			}

			return service.New(cmd.OutOrStdout(),
				fx.NopLogger,
				otlp.FXModuleFromFlags(cmd),
				otlptraces.FXModuleFromFlags(cmd),
				otlpmetrics.FXModuleFromFlags(cmd),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
				storage.NewFXModule(storage.ModuleConfig{}),
				drivers.NewFXModule(),
				fx.Invoke(all.Register),
				newWorkerModule(*workerConfiguration),
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

func newWorkerModule(configuration workerConfiguration) fx.Option {
	return worker.NewFXModule(worker.ModuleConfig{
		AsyncBlockRunnerConfig: configuration.AsyncBlockRunnerConfig,
		ReplicationConfig: configuration.ReplicationConfig,
	})
}