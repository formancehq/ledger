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
	// RequestShutdown stops the application. It is required: a server whose
	// Serve returns after a successful bind leaves the process alive with a
	// dead endpoint, and /readyz only reports Node.IsStarted(), so nothing else
	// would notice. fx.Shutdowner.Shutdown satisfies it.
	RequestShutdown func() error
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
//
// A Serve that fails AFTER the bind cannot be reported through OnStart, which
// has already returned. Deferring it to OnStop is not enough either: the
// process would keep running — Ready, since /readyz only reflects
// Node.IsStarted() — with a dead Raft or service endpoint until something else
// stopped it. The serve goroutine therefore logs the failure and requests a
// shutdown as soon as it happens, and OnStop still returns the error so the
// exit is not silent. Replacing the panic must not cost the loud failure the
// panic provided.
func grpcServerHook(cfg grpcServerHookConfig) fx.Hook {
	var (
		wait     func()
		serveErr error
	)

	return fx.Hook{
		OnStart: func(_ context.Context) error {
			// Unreachable by contract: every call site passes
			// fx.Shutdowner.Shutdown. Refuse to start rather than serve with
			// the failure path silently disabled.
			if cfg.RequestShutdown == nil {
				return fmt.Errorf("%s: no shutdown requester wired into the hook", cfg.Name)
			}

			if err := cfg.Server.Listen(); err != nil {
				return fmt.Errorf("%s: %w", cfg.Name, err)
			}

			wait = otlplogs.GoWait(func() {
				serveErr = cfg.Server.Serve()
				if serveErr == nil {
					// A normal shutdown returns nil: serveSingle and serveDual
					// tolerate ErrServerStopped and net.ErrClosed, and Serve
					// returns nil when Stop closed the listener before the
					// goroutine ran. So non-nil means the endpoint died under a
					// live application.
					return
				}

				cfg.Logger.Errorf("%s stopped serving: %v", cfg.Name, serveErr)

				if err := cfg.RequestShutdown(); err != nil {
					// Nothing left to escalate to; make the double failure
					// visible instead of exiting on the OnStop error alone.
					cfg.Logger.Errorf("%s: requesting application shutdown: %v", cfg.Name, err)
				}
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

			// OnStart sets wait before returning, and fx only stops hooks whose
			// OnStart succeeded: it increments numStarted after the call returns
			// nil and walks backward from that count. A nil wait here means the
			// contract broke, so say so rather than silently skipping the join
			// and leaking the serve goroutine.
			if wait == nil {
				return fmt.Errorf("%s: OnStop ran without a completed OnStart", cfg.Name)
			}

			// GoWait closes its done channel after the goroutine returns and
			// this call blocks on it, which is the happens-before edge that
			// makes reading serveErr race-free under -race.
			wait()

			if err != nil {
				return fmt.Errorf("%s: %w", cfg.Name, err)
			}

			// Already names the server: serveSingle and serveDual wrap as
			// "<name> server failed".
			return serveErr
		},
	}
}

// shutdownRequester adapts fx.Shutdowner to the hook's RequestShutdown, whose
// signature stays free of fx so the hook can be tested with a plain spy.
func shutdownRequester(shutdowner fx.Shutdowner) func() error {
	return func() error {
		return shutdowner.Shutdown()
	}
}
