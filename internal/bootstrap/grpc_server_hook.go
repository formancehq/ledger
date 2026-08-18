package bootstrap

import (
	"context"
	"fmt"

	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/infra/monitoring/otlplogs"
)

// grpcServer is the part of the gRPC adapter servers a lifecycle hook needs.
// RaftServer and ServiceServer both satisfy it.
type grpcServer interface {
	Listen() error
	Serve() error
	Stop() error
}

// grpcServerHookConfig configures grpcServerHook.
type grpcServerHookConfig struct {
	Server grpcServer
	// Name appears in the log lines and in the wrapped startup error.
	Name   string
	Logger logging.Logger
	// AfterListen runs once the port is bound, before OnStart returns. The Raft
	// hook uses it for membership.Start(), which must not run until the local
	// Raft listener exists. net.Listen already puts the socket in the listen
	// state and the kernel queues connections before Accept, so this is the
	// same ordering the old close(listening) signal provided.
	AfterListen func()
}

// grpcServerHook returns the fx lifecycle hook that binds, serves and stops a
// gRPC server.
//
// The bind is synchronous in OnStart, so a failure (an occupied port, most
// often) aborts startup with a wrapped error. It used to run inside the serving
// goroutine as panic(err): otlplogs.RecoverAndLogPanics re-panics after
// flushing, so the process died and, under Ginkgo, took the report of the
// genuinely failing spec with it (EN-1784). The old `listening` channel was
// never closed on a bind failure, so OnStart could not observe the error at
// all — it would have hung had the panic not won the race.
func grpcServerHook(cfg grpcServerHookConfig) fx.Hook {
	var (
		wait     func()
		serveErr error
	)

	return fx.Hook{
		OnStart: func(_ context.Context) error {
			cfg.Logger.Infof("Starting %s", cfg.Name)

			if err := cfg.Server.Listen(); err != nil {
				return fmt.Errorf("%s: %w", cfg.Name, err)
			}

			wait = otlplogs.GoWait(func() {
				serveErr = cfg.Server.Serve()
			}, cfg.Logger)

			if cfg.AfterListen != nil {
				cfg.AfterListen()
			}

			cfg.Logger.Infof("%s started successfully", cfg.Name)

			return nil
		},
		OnStop: func(_ context.Context) error {
			cfg.Logger.Infof("Stopping %s", cfg.Name)

			err := cfg.Server.Stop()

			// GoWait closes its done channel after the goroutine returns and
			// this call blocks on it, which is the happens-before edge that
			// makes reading serveErr race-free under -race.
			if wait != nil {
				wait()
			}

			if err != nil {
				return err
			}

			return serveErr
		},
	}
}
