package cmd

import (
	"github.com/formancehq/go-libs/v2/bun/bunconnect"
	"github.com/formancehq/go-libs/v2/otlp"
	"github.com/formancehq/go-libs/v2/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v2/otlp/otlptraces"
	"github.com/formancehq/go-libs/v2/service"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/drivers/all"
	"github.com/formancehq/ledger/internal/replication/runner"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"time"
)

const (
	PipelinesSyncPeriodFlag      = "pipelines-sync-period"
	PipelinesPullIntervalFlag    = "pipelines-pull-interval"
	PipelinesPushRetryPeriodFlag = "pipelines-push-retry-period"
)

func addWorkerFlags(cmd *cobra.Command) {
	cmd.Flags().Duration(PipelinesSyncPeriodFlag, 5*time.Second, "Pipelines sync period")
	cmd.Flags().Duration(PipelinesPullIntervalFlag, runner.DefaultPullInterval, "Pipelines pull interval")
	cmd.Flags().Duration(PipelinesPushRetryPeriodFlag, runner.DefaultPushRetryPeriod, "Pipelines push retry period")
}

type workerConfiguration struct {
	pipelinesSyncPeriod      time.Duration
	pipelinesPullInterval    time.Duration
	pipelinesPushRetryPeriod time.Duration
}

func discoverWorkerConfiguration(cmd *cobra.Command) workerConfiguration {
	ret := workerConfiguration{}
	ret.pipelinesSyncPeriod, _ = cmd.Flags().GetDuration(PipelinesSyncPeriodFlag)
	ret.pipelinesPullInterval, _ = cmd.Flags().GetDuration(PipelinesPullIntervalFlag)
	ret.pipelinesPushRetryPeriod, _ = cmd.Flags().GetDuration(PipelinesPushRetryPeriodFlag)

	return ret
}

func NewWorkerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "worker",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			configuration := discoverWorkerConfiguration(cmd)

			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}

			return service.New(cmd.OutOrStdout(),
				fx.NopLogger,
				otlp.FXModuleFromFlags(cmd),
				otlptraces.FXModuleFromFlags(cmd),
				otlpmetrics.FXModuleFromFlags(cmd),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
				storage.NewFXModule(false),
				newWorkerModule(configuration),
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
	return fx.Options(
		drivers.NewFXModule(),
		fx.Invoke(all.Register),
		runner.NewFXModule(runner.ModuleConfig{
			SyncPeriod:      configuration.pipelinesSyncPeriod,
			PullInterval:    configuration.pipelinesPullInterval,
			PushRetryPeriod: configuration.pipelinesPushRetryPeriod,
		}),
	)
}
