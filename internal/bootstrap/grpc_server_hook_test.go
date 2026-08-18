package bootstrap

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

type fakeGRPCServer struct {
	listenErr error
	serveErr  error
	stopErr   error
	served    bool
	stopped   bool
}

func (f *fakeGRPCServer) Listen() error { return f.listenErr }

func (f *fakeGRPCServer) Serve() error {
	f.served = true

	return f.serveErr
}

func (f *fakeGRPCServer) Stop() error {
	f.stopped = true

	return f.stopErr
}

func TestGRPCServerHookReturnsBindErrorFromOnStart(t *testing.T) {
	t.Parallel()

	bindErr := errors.New("bind: address already in use")
	srv := &fakeGRPCServer{listenErr: bindErr}

	hook := grpcServerHook(grpcServerHookConfig{
		Server: srv,
		Name:   "Raft gRPC server",
		Logger: logging.Testing(),
	})

	// EN-1784: a bind failure used to panic on a background goroutine. It must
	// now abort startup through the fx lifecycle.
	var err error
	require.NotPanics(t, func() { err = hook.OnStart(context.Background()) })

	require.ErrorIs(t, err, bindErr)
	require.Contains(t, err.Error(), "Raft gRPC server")
	require.False(t, srv.served, "Serve must not run when Listen failed")
}

func TestGRPCServerHookRunsAfterListenOnlyWhenTheBindSucceeds(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name       string
		listenErr  error
		wantCalled bool
	}{
		{name: "bind succeeds", listenErr: nil, wantCalled: true},
		{name: "bind fails", listenErr: errors.New("boom"), wantCalled: false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			called := false
			srv := &fakeGRPCServer{listenErr: tc.listenErr}

			hook := grpcServerHook(grpcServerHookConfig{
				Server:      srv,
				Name:        "Raft gRPC server",
				Logger:      logging.Testing(),
				AfterListen: func() { called = true },
			})

			err := hook.OnStart(context.Background())

			if tc.listenErr == nil {
				require.NoError(t, err)

				// OnStop joins the serve goroutine, so reading served after it
				// is ordered.
				require.NoError(t, hook.OnStop(context.Background()))
				require.True(t, srv.stopped)
				require.True(t, srv.served, "Serve must run once the port is bound")
			} else {
				require.Error(t, err)
			}

			require.Equal(t, tc.wantCalled, called)
		})
	}
}

// TestGRPCServerHookReturnsServeErrorFromOnStop pins the path that delivered
// EN-1784's bogus error to fx: whatever the serve goroutine returns has to
// travel back through OnStop, so a Serve that fails is not swallowed.
func TestGRPCServerHookReturnsServeErrorFromOnStop(t *testing.T) {
	t.Parallel()

	serveErr := errors.New("Raft gRPC server failed: boom")
	srv := &fakeGRPCServer{serveErr: serveErr}

	hook := grpcServerHook(grpcServerHookConfig{
		Server: srv,
		Name:   "Raft gRPC server",
		Logger: logging.Testing(),
	})

	require.NoError(t, hook.OnStart(context.Background()))
	require.ErrorIs(t, hook.OnStop(context.Background()), serveErr)
	require.True(t, srv.served)
}

func TestGRPCServerHookWrapsStopErrorWithTheServerName(t *testing.T) {
	t.Parallel()

	stopErr := errors.New("stop failed")
	srv := &fakeGRPCServer{stopErr: stopErr}

	hook := grpcServerHook(grpcServerHookConfig{
		Server: srv,
		Name:   "Service gRPC server",
		Logger: logging.Testing(),
	})

	require.NoError(t, hook.OnStart(context.Background()))

	err := hook.OnStop(context.Background())
	require.ErrorIs(t, err, stopErr)
	require.Contains(t, err.Error(), "Service gRPC server",
		"the shutdown error must name which server failed to stop")
}

// TestGRPCServerHookStopErrorWinsOverServeError pins the precedence: a failure
// to stop is the more actionable of the two, and Serve returning an error is
// expected once the listener is torn down underneath it.
func TestGRPCServerHookStopErrorWinsOverServeError(t *testing.T) {
	t.Parallel()

	stopErr := errors.New("stop failed")
	serveErr := errors.New("serve failed")
	srv := &fakeGRPCServer{stopErr: stopErr, serveErr: serveErr}

	hook := grpcServerHook(grpcServerHookConfig{
		Server: srv,
		Name:   "Service gRPC server",
		Logger: logging.Testing(),
	})

	require.NoError(t, hook.OnStart(context.Background()))

	err := hook.OnStop(context.Background())
	require.ErrorIs(t, err, stopErr)
	require.NotErrorIs(t, err, serveErr)
}

// TestGRPCServerHookOnStopWithoutOnStartFailsLoudly covers the branch that fx
// cannot reach: it increments numStarted only after OnStart returns nil and
// walks backward from that count, so OnStop never runs for a hook that did not
// start. An unreachable-by-contract branch must still surface the violation
// rather than silently skip joining the serve goroutine.
func TestGRPCServerHookOnStopWithoutOnStartFailsLoudly(t *testing.T) {
	t.Parallel()

	srv := &fakeGRPCServer{}

	hook := grpcServerHook(grpcServerHookConfig{
		Server: srv,
		Name:   "Raft gRPC server",
		Logger: logging.Testing(),
	})

	err := hook.OnStop(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "OnStop ran without a completed OnStart")
	require.Contains(t, err.Error(), "Raft gRPC server")
}
