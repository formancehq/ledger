package api

import (
	"context"
	"net"
	"net/http"
)

func StartServer(ctx context.Context, addr string, handler http.Handler) error {
	socket, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}

	tcpAddr, err := net.ResolveTCPAddr(socket.Addr().Network(), socket.Addr().String())
	if err != nil {
		panic(err)
	}

	setPort(ctx, tcpAddr.Port)

	go func() {
		err := http.Serve(socket, handler)
		if err != nil {
			panic(err)
		}
	}()
	return nil
}
