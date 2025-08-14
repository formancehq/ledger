package worker

import (
	"fmt"
	"github.com/formancehq/go-libs/v3/grpcserver"
	"github.com/formancehq/go-libs/v3/serverport"
	"github.com/formancehq/ledger/internal/replication"
	innergrpc "github.com/formancehq/ledger/internal/replication/grpc"
	"github.com/formancehq/ledger/internal/storage/workers/asyncblock"
	"github.com/formancehq/ledger/internal/storage/workers/lockmonitor"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"
	"google.golang.org/grpc"
)

type GRPCServerModuleConfig struct {
	Address       string
	ServerOptions []grpc.ServerOption
}

type ModuleConfig struct {
	AsyncBlockRunnerConfig asyncblock.Config
	ReplicationConfig      replication.WorkerModuleConfig
	LockMonitorConfig      lockmonitor.Config
}

func NewFXModule(cfg ModuleConfig) fx.Option {
	return fx.Options(
		// todo: add auto discovery
		asyncblock.NewModule(cfg.AsyncBlockRunnerConfig),
		replication.NewWorkerFXModule(cfg.ReplicationConfig),
		lockmonitor.NewModule(cfg.LockMonitorConfig),
	)
}

func NewGRPCServerFXModule(cfg GRPCServerModuleConfig) fx.Option {
	return fx.Options(
		fx.Invoke(func(lc fx.Lifecycle, replicationServer innergrpc.ReplicationServer, traceProvider trace.TracerProvider) {
			lc.Append(grpcserver.NewHook(
				grpcserver.WithServerPortOptions(
					serverport.WithAddress(cfg.Address),
				),
				grpcserver.WithGRPCSetupOptions(func(server *grpc.Server) {
					innergrpc.RegisterReplicationServer(server, replicationServer)
				}),
				grpcserver.WithGRPCServerOptions(
					grpc.StatsHandler(otelgrpc.NewServerHandler(otelgrpc.WithTracerProvider(traceProvider))),
				),
			))
		}),
	)
}

func NewGRPCClientFxModule(
	address string,
	options ...grpc.DialOption,
) fx.Option {
	return fx.Options(
		fx.Provide(func(tracerProvider trace.TracerProvider) (*grpc.ClientConn, error) {
			client, err := grpc.NewClient(address, append(
				options,
				grpc.WithStatsHandler(otelgrpc.NewClientHandler(otelgrpc.WithTracerProvider(tracerProvider))),
			)...)
			if err != nil {
				return nil, fmt.Errorf("failed to dial: %v", err)
			}

			return client, nil
		}),
	)
}
