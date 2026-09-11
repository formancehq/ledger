package bootstrap

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

func TestHTTPServerHookOccupiedAddressFailsStart(t *testing.T) {
	t.Parallel()
	occupied := mustListenLoopback(t, 0)
	var shutdowns atomic.Int32
	hook := httpServerHook(http.NotFoundHandler(), nil, occupied.Addr().String(), logging.Testing(), func() error {
		shutdowns.Add(1)

		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	err := hook.OnStart(ctx)
	require.Error(t, err)
	var opErr *net.OpError
	require.ErrorAs(t, err, &opErr)
	require.Equal(t, "listen", opErr.Op)
	require.Zero(t, shutdowns.Load(), "synchronous bind failures must be returned by OnStart")
}

func TestHTTPServerHookGracefulStopDrainsRequest(t *testing.T) {
	t.Parallel()
	listener := &httpControlListener{Listener: mustListenLoopback(t, 0), closed: make(chan struct{})}
	entered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var shutdowns atomic.Int32
	hook := httpServerHook(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		close(entered)
		<-release
		_, _ = io.WriteString(w, "drained") // The response is verified by the client below.
	}), listener, "", logging.Testing(), func() error {
		shutdowns.Add(1)

		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, hook.OnStart(ctx))
	defer func() {
		releaseOnce.Do(func() { close(release) })
		require.NoError(t, hook.OnStop(ctx))
	}()
	type result struct {
		body string
		err  error
	}
	response := make(chan result, 1)
	go func() {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://"+listener.Addr().String(), nil)
		if err != nil {
			response <- result{err: err}

			return
		}
		res, err := http.DefaultClient.Do(request)
		if err != nil {
			response <- result{err: err}

			return
		}
		body, err := io.ReadAll(res.Body)
		err = errors.Join(err, res.Body.Close())
		response <- result{body: string(body), err: err}
	}()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("request did not reach the handler")
	}
	stopped := make(chan error, 1)
	stopDone := make(chan struct{})
	go func() {
		defer close(stopDone)
		stopped <- hook.OnStop(ctx)
	}()
	defer func() {
		releaseOnce.Do(func() { close(release) })
		<-stopDone
	}()
	select {
	case <-listener.closed:
	case <-ctx.Done():
		t.Fatal("shutdown did not close the listener")
	}
	select {
	case err := <-stopped:
		t.Fatalf("shutdown returned before the active request drained: %v", err)
	default:
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case got := <-response:
		require.NoError(t, got.err)
		require.Equal(t, "drained", got.body)
	case <-ctx.Done():
		t.Fatal("active request did not complete")
	}
	select {
	case err := <-stopped:
		require.NoError(t, err)
	case <-ctx.Done():
		t.Fatal("shutdown did not finish after request drained")
	}
	require.Zero(t, shutdowns.Load(), "normal shutdown must not be reported as a serving failure")
}

func TestHTTPServerHookRetriesTemporaryAcceptError(t *testing.T) {
	t.Parallel()
	listener := &httpControlListener{
		Listener: mustListenLoopback(t, 0),
		firstErr: &net.DNSError{Err: "temporary accept failure", IsTemporary: true},
	}
	var shutdowns atomic.Int32
	hook := httpServerHook(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}), listener, "", logging.Testing(), func() error {
		shutdowns.Add(1)

		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, hook.OnStart(ctx))
	defer func() { require.NoError(t, hook.OnStop(ctx)) }()
	requireHTTPOK(t, listener.Addr().String())
	require.GreaterOrEqual(t, listener.accepts.Load(), int32(2), "the injected failure must be retried before serving a request")
	require.NoError(t, hook.OnStop(ctx))
	require.Zero(t, shutdowns.Load())
}

func TestHTTPServerHookCanceledStopRetainsServeFailure(t *testing.T) {
	t.Parallel()
	failure := errors.New("permanent accept failure")
	listener := &httpControlListener{Listener: mustListenLoopback(t, 0), firstErr: failure}
	shutdown := make(chan struct{})
	hook := httpServerHook(http.NotFoundHandler(), listener, "", logging.Testing(), func() error {
		close(shutdown)

		return nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	require.NoError(t, hook.OnStart(ctx))
	select {
	case <-shutdown:
	case <-ctx.Done():
		t.Fatal("permanent serving failure did not request shutdown")
	}
	stopCtx, stopCancel := context.WithCancel(context.Background())
	stopCancel()
	require.ErrorIs(t, hook.OnStop(stopCtx), failure)
	require.Equal(t, int32(1), listener.accepts.Load())
}

// httpControlListener injects a single Accept failure and exposes listener closure
// while preserving real TCP connections and net/http's serving lifecycle.
type httpControlListener struct {
	net.Listener

	firstErr error
	accepts  atomic.Int32
	closed   chan struct{}
	once     sync.Once
}

func (l *httpControlListener) Accept() (net.Conn, error) {
	if l.accepts.Add(1) == 1 && l.firstErr != nil {
		return nil, l.firstErr
	}

	return l.Listener.Accept()
}

func (l *httpControlListener) Close() error {
	err := l.Listener.Close()
	if l.closed != nil {
		l.once.Do(func() { close(l.closed) })
	}

	return err
}
