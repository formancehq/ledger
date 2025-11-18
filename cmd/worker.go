package cmd

import (
	"fmt"
	"time"

	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/go-libs/v3/bun/bunconnect"
	"github.com/formancehq/go-libs/v3/otlp/otlpmetrics"
	"github.com/formancehq/go-libs/v3/otlp/otlptraces"
	"github.com/formancehq/go-libs/v3/service"

	"github.com/formancehq/ledger/internal/replication"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"github.com/formancehq/ledger/internal/replication/drivers/alldrivers"
	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/ledger/internal/worker"
)

const (
	WorkerPipelinesPullIntervalFlag    = "worker-pipelines-pull-interval"
	WorkerPipelinesPushRetryPeriodFlag = "worker-pipelines-push-retry-period"
	WorkerPipelinesSyncPeriod          = "worker-pipelines-sync-period"
	WorkerPipelinesLogsPageSize        = "worker-pipelines-logs-page-size"

	WorkerAsyncBlockHasherMaxBlockSizeFlag = "worker-async-block-hasher-max-block-size"
	WorkerAsyncBlockHasherScheduleFlag     = "worker-async-block-hasher-schedule"

	WorkerBucketCleanupRetentionPeriodFlag = "worker-bucket-cleanup-retention-period"
	WorkerBucketCleanupScheduleFlag        = "worker-bucket-cleanup-schedule"

	WorkerGRPCAddressFlag = "worker-grpc-address"
)

type WorkerGRPCConfig struct {
	Address string `mapstructure:"worker-grpc-address"`
}

type WorkerConfiguration struct {
	HashLogsBlockMaxSize  int           `mapstructure:"worker-async-block-hasher-max-block-size"`
	HashLogsBlockCRONSpec cron.Schedule `mapstructure:"worker-async-block-hasher-schedule"`

	PushRetryPeriod time.Duration `mapstructure:"worker-pipelines-push-retry-period"`
	PullInterval    time.Duration `mapstructure:"worker-pipelines-pull-interval"`
	SyncPeriod      time.Duration `mapstructure:"worker-pipelines-sync-period"`
	LogsPageSize    uint64        `mapstructure:"worker-pipelines-logs-page-size"`

	BucketCleanupRetentionPeriod time.Duration `mapstructure:"worker-bucket-cleanup-retention-period"`
	BucketCleanupCRONSpec        cron.Schedule `mapstructure:"worker-bucket-cleanup-schedule"`
}

func (cfg WorkerConfiguration) Validate() error {
	if cfg.BucketCleanupRetentionPeriod <= 0 {
		return fmt.Errorf("bucket cleanup retention period must be greater than zero")
	}
	if cfg.BucketCleanupCRONSpec == nil {
		return fmt.Errorf("bucket cleanup schedule must be set")
	}

	return nil
}

type WorkerCommandConfiguration struct {
	WorkerConfiguration `mapstructure:",squash"`
	commonConfig        `mapstructure:",squash"`
	WorkerGRPCConfig    `mapstructure:",squash"`
}

// addWorkerFlags adds command-line flags to cmd to configure worker runtime behavior.
// The flags control async block hashing, pipeline pull/push/sync behavior and pagination, and bucket cleanup retention and schedule.
func addWorkerFlags(cmd *cobra.Command) {
	cmd.Flags().Int(WorkerAsyncBlockHasherMaxBlockSizeFlag, 1000, "Max block size")
	cmd.Flags().String(WorkerAsyncBlockHasherScheduleFlag, "0 * * * * *", "Schedule")
	cmd.Flags().Duration(WorkerPipelinesPullIntervalFlag, 5*time.Second, "Pipelines pull interval")
	cmd.Flags().Duration(WorkerPipelinesPushRetryPeriodFlag, 10*time.Second, "Pipelines push retry period")
	cmd.Flags().Duration(WorkerPipelinesSyncPeriod, time.Minute, "Pipelines sync period")
	cmd.Flags().Uint64(WorkerPipelinesLogsPageSize, 100, "Pipelines logs page size")
	cmd.Flags().Duration(WorkerBucketCleanupRetentionPeriodFlag, 30*24*time.Hour, "Retention period for deleted buckets before hard delete")
	cmd.Flags().String(WorkerBucketCleanupScheduleFlag, "0 0 * * * *", "Schedule for bucket cleanup (cron format)")
}

// NewWorkerCommand constructs the "worker" Cobra command which initializes and runs the worker service using loaded configuration and composed FX modules.
// The command registers worker-specific flags via addWorkerFlags and common service, bunconnect, and OTLP flags, and exposes the --worker-grpc-address flag (default ":8081").
// When executed it loads configuration and starts the service with the configured modules and a gRPC server.
func NewWorkerCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:          "worker",
		SilenceUsage: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd)
			if err != nil {
				return err
			}

			cfg, err := LoadConfig[WorkerCommandConfiguration](cmd)
			if err != nil {
				return fmt.Errorf("loading config: %w", err)
			}

			if err := cfg.Validate(); err != nil {
				return err
			}

			return service.New(cmd.OutOrStdout(),
				fx.NopLogger,
				otlpModule(cmd, cfg.commonConfig),
				bunconnect.Module(*connectionOptions, service.IsDebug(cmd)),
				storage.NewFXModule(storage.ModuleConfig{}),
				drivers.NewFXModule(),
				fx.Invoke(alldrivers.Register),
				newWorkerModule(cfg.WorkerConfiguration),
				worker.NewGRPCServerFXModule(worker.GRPCServerModuleConfig{
					Address: cfg.Address,
					ServerOptions: []grpc.ServerOption{
						grpc.Creds(insecure.NewCredentials()),
					},
				}),
			).Run(cmd)
		},
	}

	cmd.Flags().String(WorkerGRPCAddressFlag, ":8081", "GRPC address")

	addWorkerFlags(cmd)
	service.AddFlags(cmd.Flags())
	bunconnect.AddFlags(cmd.Flags())
	otlpmetrics.AddFlags(cmd.Flags())
	otlptraces.AddFlags(cmd.Flags())

	return cmd
}

// newWorkerModule creates an fx.Option that configures the worker module using the provided WorkerConfiguration.
// It maps the configuration into AsyncBlockRunnerConfig, ReplicationConfig, and BucketCleanupRunnerConfig for the worker.
func newWorkerModule(configuration WorkerConfiguration) fx.Option {
	return worker.NewFXModule(worker.ModuleConfig{
		AsyncBlockRunnerConfig: storage.AsyncBlockRunnerConfig{
			MaxBlockSize: configuration.HashLogsBlockMaxSize,
			Schedule:     configuration.HashLogsBlockCRONSpec,
		},
		ReplicationConfig: replication.WorkerModuleConfig{
			PushRetryPeriod: configuration.PushRetryPeriod,
			PullInterval:    configuration.PullInterval,
			SyncPeriod:      configuration.SyncPeriod,
			LogsPageSize:    configuration.LogsPageSize,
		},
		BucketCleanupRunnerConfig: storage.BucketCleanupRunnerConfig{
			RetentionPeriod: configuration.BucketCleanupRetentionPeriod,
			Schedule:        configuration.BucketCleanupCRONSpec,
		},
	})
}
