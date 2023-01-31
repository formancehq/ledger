package httpserver

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/formancehq/go-libs/logging"
	"go.uber.org/fx"
)

func NewMuxServer(addr string) (*http.ServeMux, *http.Server) {
	mux := http.NewServeMux()
	server := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: time.Second,
	}
	return mux, server
}

func RegisterHandler(mux *http.ServeMux, h http.Handler) {
	mux.Handle("/", h)
}

func Run(lc fx.Lifecycle, server *http.Server, addr string) {
	lc.Append(fx.Hook{
		OnStart: func(ctx context.Context) error {
			logging.GetLogger(ctx).Infof(fmt.Sprintf("starting HTTP listening on %s", addr))
			go func() {
				if err := server.ListenAndServe(); err != nil &&
					!errors.Is(err, http.ErrServerClosed) {
					logging.GetLogger(ctx).Errorf("http.Server.ListenAndServe: %s", err)
				}
			}()
			return nil
		},
		OnStop: func(ctx context.Context) error {
			logging.GetLogger(ctx).Infof("stopping HTTP listening")
			if err := server.Shutdown(ctx); err != nil {
				return fmt.Errorf("http.Server.Shutdown: %w", err)
			}
			return nil
		},
	})
}
