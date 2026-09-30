package server

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/fx"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestTerminationDuringStartupQueuesGracefulShutdown(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	signals := make(chan os.Signal, 1)
	done := make(chan struct{})
	defer close(done)
	ready := make(chan struct{})
	finishStartup := make(chan struct{})
	defer close(finishStartup)
	stopped := make(chan struct{})
	app := fx.New(
		fx.NopLogger,
		fx.Supply(fx.Annotate(logging.NewDefaultLogger(io.Discard, false, false, false), fx.As(new(logging.Logger)))),
		terminationSignals(signals, done),
		fx.Invoke(func(lifecycle fx.Lifecycle) {
			lifecycle.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					close(ready)
					select {
					case <-finishStartup:
						return ctx.Err()
					case <-ctx.Done():
						return ctx.Err()
					}
				},
				OnStop: func(context.Context) error {
					close(stopped)

					return nil
				},
			})
		}),
	)
	started := make(chan error, 1)
	go func() { started <- app.Start(ctx) }()
	select {
	case <-ready:
	case <-ctx.Done():
		t.Fatal("startup did not reach readiness")
	}
	signals <- os.Interrupt
	select {
	case <-app.Wait():
	case <-ctx.Done():
		t.Fatal("termination was not forwarded while startup was in progress")
	}
	finishStartup <- struct{}{}
	require.NoError(t, <-started, "termination must not cancel the startup hook")
	// Fx retains the shutdown request for the service runner's later Wait call.
	select {
	case <-app.Wait():
	case <-ctx.Done():
		t.Fatal("termination was lost before the service runner began waiting")
	}
	require.NoError(t, app.Stop(ctx))
	select {
	case <-stopped:
	default:
		t.Fatal("graceful stop hook was not run")
	}
}
