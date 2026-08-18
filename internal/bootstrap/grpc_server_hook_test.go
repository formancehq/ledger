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
	served    bool
	stopped   bool
}

func (f *fakeGRPCServer) Listen() error { return f.listenErr }

func (f *fakeGRPCServer) Serve() error {
	f.served = true

	return nil
}

func (f *fakeGRPCServer) Stop() error {
	f.stopped = true

	return nil
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

func TestGRPCServerHookRunsAfterListenOnlyOnSuccess(t *testing.T) {
	t.Parallel()

	called := false
	srv := &fakeGRPCServer{}

	hook := grpcServerHook(grpcServerHookConfig{
		Server:      srv,
		Name:        "Raft gRPC server",
		Logger:      logging.Testing(),
		AfterListen: func() { called = true },
	})

	require.NoError(t, hook.OnStart(context.Background()))
	require.True(t, called, "AfterListen must run once the port is bound")

	require.NoError(t, hook.OnStop(context.Background()))
	require.True(t, srv.stopped)
}

func TestGRPCServerHookSkipsAfterListenWhenBindFails(t *testing.T) {
	t.Parallel()

	called := false
	srv := &fakeGRPCServer{listenErr: errors.New("boom")}

	hook := grpcServerHook(grpcServerHookConfig{
		Server:      srv,
		Name:        "Service gRPC server",
		Logger:      logging.Testing(),
		AfterListen: func() { called = true },
	})

	require.Error(t, hook.OnStart(context.Background()))
	require.False(t, called, "AfterListen must not run when the bind failed")
}
