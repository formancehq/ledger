package httpserver

import (
	"context"
	"net"
	"net/http"
	"strconv"

	"go.uber.org/fx"
)

type serverInfo struct {
	started chan struct{}
	port    int
}

type serverInfoContextKey string

var serverInfoKey serverInfoContextKey = "_serverInfo"

func getActualServerInfo(ctx context.Context) *serverInfo {
	siAsAny := ctx.Value(serverInfoKey)
	if siAsAny == nil {
		return nil
	}
	return siAsAny.(*serverInfo)
}

func ContextWithServerInfo(ctx context.Context) context.Context {
	return context.WithValue(ctx, serverInfoKey, &serverInfo{
		started: make(chan struct{}),
	})
}

func Started(ctx context.Context) chan struct{} {
	si := getActualServerInfo(ctx)
	if si == nil {
		return nil
	}
	return si.started
}

func Port(ctx context.Context) int {
	si := getActualServerInfo(ctx)
	if si == nil {
		return 0
	}
	return si.port
}

func StartedServer(ctx context.Context, listener net.Listener) {
	si := getActualServerInfo(ctx)
	if si == nil {
		return
	}

	_, portAsString, _ := net.SplitHostPort(listener.Addr().String())
	port, _ := strconv.ParseInt(portAsString, 10, 32)

	si.port = int(port)
	close(si.started)
}

func StartServer(ctx context.Context, bind string, handler http.Handler, options ...func(server *http.Server)) (func(ctx context.Context) error, error) {
	listener, err := net.Listen("tcp", bind)
	if err != nil {
		return func(ctx context.Context) error {
			return nil
		}, err
	}
	StartedServer(ctx, listener)

	srv := &http.Server{
		Handler: handler,
	}
	for _, option := range options {
		option(srv)
	}

	go func() {
		err := srv.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()

	return func(ctx context.Context) error {
		return srv.Shutdown(ctx)
	}, nil
}

func NewHook(addr string, handler http.Handler, options ...func(server *http.Server)) fx.Hook {
	var (
		close func(ctx context.Context) error
		err   error
	)
	return fx.Hook{
		OnStart: func(ctx context.Context) error {
			close, err = StartServer(ctx, addr, handler, options...)
			return err
		},
		OnStop: func(ctx context.Context) error {
			if close == nil {
				return nil
			}
			return close(ctx)
		},
	}
}
