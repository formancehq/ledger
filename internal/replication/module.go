package replication

import (
	"context"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/controller/system"
	"github.com/formancehq/ledger/internal/replication/drivers"
	innergrpc "github.com/formancehq/ledger/internal/replication/grpc"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"time"
)

type WorkerModuleConfig struct {
	PushRetryPeriod time.Duration
	PullInterval    time.Duration
}

// NewWorkerFXModule create a new fx module
func NewWorkerFXModule(cfg WorkerModuleConfig) fx.Option {
	return fx.Options(
		fx.Provide(fx.Annotate(NewStorageAdapter, fx.As(new(Storage)))),
		fx.Provide(func(
			storageDriver Storage,
			connectorFactory drivers.Factory,
			connectorsConfigValidator ConfigValidator,
			logger logging.Logger,
		) *Manager {
			options := make([]Option, 0)
			if cfg.PushRetryPeriod > 0 {
				options = append(options, WithPipelineOptions(
					WithPushRetryPeriod(cfg.PushRetryPeriod),
				))
			}
			if cfg.PullInterval > 0 {
				options = append(options, WithPipelineOptions(
					WithPullPeriod(cfg.PullInterval),
				))
			}
			return NewManager(
				storageDriver,
				connectorFactory,
				logger,
				connectorsConfigValidator,
				options...,
			)
		}),
		fx.Provide(func(registry *drivers.Registry) drivers.Factory {
			return registry
		}),
		// decorate the original Factory (implemented by *Registry)
		// to abstract the fact we want to batch logs
		fx.Decorate(fx.Annotate(
			drivers.NewWithBatchingConnectorFactory,
			fx.As(new(drivers.Factory)),
		)),
		fx.Provide(fx.Annotate(NewReplicationServiceImpl, fx.As(new(innergrpc.ReplicationServer)))),
		fx.Provide(func(driversRegistry *drivers.Registry) ConfigValidator {
			return driversRegistry
		}),
		fx.Invoke(func(lc fx.Lifecycle, runner *Manager) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go runner.Run(context.WithoutCancel(ctx))
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return runner.Stop(ctx)
				},
			})
		}),
	)
}

func NewFXGRPCClientModule() fx.Option {
	return fx.Options(
		fx.Provide(func(conn *grpc.ClientConn) innergrpc.ReplicationClient {
			return innergrpc.NewReplicationClient(conn)
		}),
		fx.Provide(fx.Annotate(NewThroughGRPCBackend, fx.As(new(system.ReplicationBackend)))),
	)
}

func NewFXEmbeddedClientModule() fx.Option {
	return fx.Options(
		fx.Provide(func(manager *Manager) system.ReplicationBackend {
			return manager
		}),
	)
}
