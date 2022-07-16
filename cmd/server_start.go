package cmd

import (
	"context"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/numary/go-libs/sharedlogging"
	"github.com/numary/go-libs/sharedlogging/sharedlogginglogrus"
	"github.com/numary/go-libs/sharedotlp/pkg/sharedotlptraces"
	"github.com/numary/ledger/pkg/api"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/opentelemetry-go-extra/otellogrus"
	"go.uber.org/fx"
)

type serverContext struct {
	Port int
}

type serverContextKey struct{}

var serverContextKeyImpl = serverContextKey{}

func instructPort(ctx context.Context, port int) {
	serverCtx := ctx.Value(serverContextKeyImpl)
	if serverCtx == nil {
		return
	}
	serverCtx.(*serverContext).Port = port
}

func NewContext(ctx context.Context) context.Context {
	return context.WithValue(ctx, serverContextKeyImpl, &serverContext{})
}

func Port(ctx context.Context) int {
	serverCtx := ctx.Value(serverContextKeyImpl)
	if serverCtx == nil {
		return 0
	}
	return serverCtx.(*serverContext).Port
}

func NewServerStart() *cobra.Command {
	return &cobra.Command{
		Use: "start",
		RunE: func(cmd *cobra.Command, args []string) error {
			l := logrus.New()
			if viper.GetBool(debugFlag) {
				l.Level = logrus.DebugLevel
			}
			if viper.GetBool(sharedotlptraces.OtelTracesFlag) {
				l.AddHook(otellogrus.NewHook(otellogrus.WithLevels(
					logrus.PanicLevel,
					logrus.FatalLevel,
					logrus.ErrorLevel,
					logrus.WarnLevel,
				)))
			}
			loggerFactory := sharedlogging.StaticLoggerFactory(sharedlogginglogrus.New(l))
			sharedlogging.SetFactory(loggerFactory)

			app := NewContainer(
				viper.GetViper(),
				cmd.OutOrStdout(),
				fx.Invoke(func(lc fx.Lifecycle, h *api.API) {
					var (
						err      error
						listener net.Listener
					)
					lc.Append(fx.Hook{
						OnStart: func(ctx context.Context) error {
							listener, err = net.Listen("tcp", viper.GetString(serverHttpBindAddressFlag))
							if err != nil {
								return err
							}

							addrParts := strings.Split(listener.Addr().String(), ":")
							port, err := strconv.ParseUint(addrParts[len(addrParts)-1], 10, 16)
							if err != nil {
								panic(err)
							}
							instructPort(cmd.Context(), int(port))
							go func() {
								httpErr := http.Serve(listener, h)
								sharedlogging.Errorf("http.Serve: %s", httpErr)
							}()
							return nil
						},
						OnStop: func(ctx context.Context) error {
							return listener.Close()
						},
					})
				}),
			)
			errCh := make(chan error, 1)
			go func() {
				err := app.Start(cmd.Context())
				if err != nil {
					errCh <- err
				}
			}()
			select {
			case err := <-errCh:
				return err
			case <-cmd.Context().Done():
				return app.Stop(context.Background())
			case <-app.Done():
				return app.Err()
			}
		},
	}
}
