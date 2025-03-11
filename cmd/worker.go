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
	WorkerAsyncBlockHasherMaxBlockSizeFlag       = "worker-async-block-hasher-max-block-size"
	WorkerAsyncBlockHasherScheduleFlag           = "worker-async-block-hasher-schedule"
	WorkerCleanupDeletedBucketsScheduleFlag      = "worker-cleanup-deleted-buckets-schedule"
	WorkerCleanupDeletedBucketsRetentionDaysFlag = "worker-cleanup-deleted-buckets-retention-days"
)

type WorkerConfiguration struct {
	HashLogsBlockMaxSize  int    `mapstructure:"worker-async-block-hasher-max-block-size"`
	HashLogsBlockCRONSpec string `mapstructure:"worker-async-block-hasher-schedule"`
	CleanupSchedule       string `mapstructure:"worker-cleanup-deleted-buckets-schedule"`
	CleanupRetentionDays  int    `mapstructure:"worker-cleanup-deleted-buckets-retention-days"`
}

func addWorkerFlags(cmd *cobra.Command) {
	cmd.Flags().Int(WorkerAsyncBlockHasherMaxBlockSizeFlag, 1000, "Max block size")
	cmd.Flags().String(WorkerAsyncBlockHasherScheduleFlag, "0 * * * * *", "Schedule")
	cmd.Flags().String(WorkerCleanupDeletedBucketsScheduleFlag, "0 0 * * * *", "Schedule for cleanup of deleted buckets (default: every hour)")
	cmd.Flags().Int(WorkerCleanupDeletedBucketsRetentionDaysFlag, 30, "Number of days to retain deleted buckets before physical deletion (default: 30 days)")
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
					MaxBlockSize:         cfg.HashLogsBlockMaxSize,
					Schedule:             cfg.HashLogsBlockCRONSpec,
					CleanupSchedule:      cfg.CleanupSchedule,
					CleanupRetentionDays: cfg.CleanupRetentionDays,
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
