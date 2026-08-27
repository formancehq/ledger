package bootstrap

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

// shutdownSpy stands in for fx.Shutdowner. The serve goroutine calls it, so the
// counter is read under the mutex.
type shutdownSpy struct {
	mu    sync.Mutex
	calls int
	err   error
}

func (s *shutdownSpy) request() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++

	return s.err
}

func (s *shutdownSpy) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls
}

type fakeGRPCServer struct {
	listenErr error
	serveErr  error
	stopErr   error
	listened  bool
	served    bool
	stopped   bool
}

func (f *fakeGRPCServer) Listen() error {
	f.listened = true

	return f.listenErr
}

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
		Server:          srv,
		Name:            "Raft gRPC server",
		Logger:          logging.Testing(),
		RequestShutdown: func() error { return nil },
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
				Server:          srv,
				Name:            "Raft gRPC server",
				Logger:          logging.Testing(),
				AfterListen:     func() { called = true },
				RequestShutdown: func() error { return nil },
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
		Server:          srv,
		Name:            "Raft gRPC server",
		Logger:          logging.Testing(),
		RequestShutdown: func() error { return nil },
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
		Server:          srv,
		Name:            "Service gRPC server",
		Logger:          logging.Testing(),
		RequestShutdown: func() error { return nil },
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
		Server:          srv,
		Name:            "Service gRPC server",
		Logger:          logging.Testing(),
		RequestShutdown: func() error { return nil },
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
		Server:          srv,
		Name:            "Raft gRPC server",
		Logger:          logging.Testing(),
		RequestShutdown: func() error { return nil },
	})

	err := hook.OnStop(context.Background())
	require.Error(t, err)
	require.Contains(t, err.Error(), "OnStop ran without a completed OnStart")
	require.Contains(t, err.Error(), "Raft gRPC server")
}

// TestGRPCServerHookRequestsShutdownWhenServeFailsAfterTheBind pins the reason
// the hook takes a shutdown requester at all: OnStart has already returned by
// the time Serve dies, and a process left running with a dead Raft or service
// endpoint still answers /readyz, which only reflects Node.IsStarted(). The
// request must not wait for OnStop.
func TestGRPCServerHookRequestsShutdownWhenServeFailsAfterTheBind(t *testing.T) {
	t.Parallel()

	spy := &shutdownSpy{}
	srv := &fakeGRPCServer{serveErr: errors.New("accept: too many open files")}

	hook := grpcServerHook(grpcServerHookConfig{
		Server:          srv,
		Name:            "Raft gRPC server",
		Logger:          logging.Testing(),
		RequestShutdown: spy.request,
	})

	require.NoError(t, hook.OnStart(context.Background()))

	require.Eventually(t, func() bool { return spy.count() == 1 },
		5*time.Second, 10*time.Millisecond,
		"the serve goroutine must request the shutdown itself, not leave it to OnStop")

	require.Error(t, hook.OnStop(context.Background()))
	require.Equal(t, 1, spy.count(), "OnStop must not request a second shutdown")
}

// TestGRPCServerHookDoesNotRequestShutdownOnANormalStop covers the other side:
// Serve returns nil for every expected shutdown error, so the normal path must
// not ask the application to stop.
func TestGRPCServerHookDoesNotRequestShutdownOnANormalStop(t *testing.T) {
	t.Parallel()

	spy := &shutdownSpy{}
	srv := &fakeGRPCServer{}

	hook := grpcServerHook(grpcServerHookConfig{
		Server:          srv,
		Name:            "Service gRPC server",
		Logger:          logging.Testing(),
		RequestShutdown: spy.request,
	})

	require.NoError(t, hook.OnStart(context.Background()))
	require.NoError(t, hook.OnStop(context.Background()))
	require.Zero(t, spy.count())
}

// TestGRPCServerHookStillReportsTheServeErrorWhenTheShutdownRequestFails keeps
// the two failure paths independent: a Shutdowner that cannot deliver its signal
// must not swallow the error that made the hook call it.
func TestGRPCServerHookStillReportsTheServeErrorWhenTheShutdownRequestFails(t *testing.T) {
	t.Parallel()

	serveErr := errors.New("serve failed")
	spy := &shutdownSpy{err: errors.New("no shutdown receivers")}
	srv := &fakeGRPCServer{serveErr: serveErr}

	hook := grpcServerHook(grpcServerHookConfig{
		Server:          srv,
		Name:            "Raft gRPC server",
		Logger:          logging.Testing(),
		RequestShutdown: spy.request,
	})

	require.NoError(t, hook.OnStart(context.Background()))
	require.ErrorIs(t, hook.OnStop(context.Background()), serveErr)
	require.Equal(t, 1, spy.count())
}

// TestGRPCServerHookRefusesToStartWithoutAShutdownRequester covers a branch fx
// cannot reach, since every call site passes fx.Shutdowner.Shutdown. A missing
// requester silently disables the failure path, so it must abort startup rather
// than bind and serve.
func TestGRPCServerHookRefusesToStartWithoutAShutdownRequester(t *testing.T) {
	t.Parallel()

	srv := &fakeGRPCServer{}

	hook := grpcServerHook(grpcServerHookConfig{
		Server: srv,
		Name:   "Raft gRPC server",
		Logger: logging.Testing(),
	})

	err := hook.OnStart(context.Background())
	require.ErrorContains(t, err, "no shutdown requester")
	require.Contains(t, err.Error(), "Raft gRPC server")
	require.False(t, srv.listened, "the port must not be bound when the hook is misconfigured")
	require.False(t, srv.served)
}
