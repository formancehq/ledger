package worker

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger/internal/replication"
	innergrpc "github.com/formancehq/ledger/internal/replication/grpc"
	"github.com/formancehq/ledger/internal/storage"
	"go.uber.org/fx"
	"google.golang.org/grpc"
	"net"
)

type GRPCServerModuleConfig struct {
	Address       string
	ServerOptions []grpc.ServerOption
}

type ModuleConfig struct {
	AsyncBlockRunnerConfig storage.AsyncBlockRunnerConfig
	ReplicationConfig      replication.WorkerModuleConfig
}

func NewFXModule(cfg ModuleConfig) fx.Option {
	return fx.Options(
		// todo: add auto discovery
		storage.NewAsyncBlockRunnerModule(cfg.AsyncBlockRunnerConfig),
		// todo: add auto discovery
		replication.NewWorkerFXModule(cfg.ReplicationConfig),
	)
}

func NewGRPCServerFXModule(cfg GRPCServerModuleConfig) fx.Option {
	return fx.Options(
		fx.Invoke(func(lc fx.Lifecycle, replicationServer innergrpc.ReplicationServer) {
			var grpcServer *grpc.Server
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					logging.FromContext(ctx).Infof("starting grpc server on %s", cfg.Address)
					lis, err := net.Listen("tcp", cfg.Address)
					if err != nil {
						return fmt.Errorf("failed to listen: %v", err)
					}
					grpcServer = grpc.NewServer(cfg.ServerOptions...)
					// todo: add auto discovery
					innergrpc.RegisterReplicationServer(grpcServer, replicationServer)
					go func() {
						if err := grpcServer.Serve(lis); err != nil {
							logging.FromContext(ctx).Errorf("failed to serve: %v", err)
						}
					}()

					return nil
				},
				OnStop: func(ctx context.Context) error {
					if grpcServer == nil {
						return nil
					}
					grpcServer.GracefulStop()

					return nil
				},
			})
		}),
	)
}

func NewGRPCClientFxModule(
	address string,
	options ...grpc.DialOption,
) fx.Option {
	return fx.Options(
		fx.Provide(func() (*grpc.ClientConn, error) {
			client, err := grpc.NewClient(address, options...)
			if err != nil {
				return nil, fmt.Errorf("failed to dial: %v", err)
			}

			return client, nil
		}),
	)
}
