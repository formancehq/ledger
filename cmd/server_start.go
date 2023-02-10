package cmd

import (
	"context"
	"net"
	"net/http"

	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/numary/ledger/pkg/api"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

func NewServerStart() *cobra.Command {
	return &cobra.Command{
		Use: "start",
		RunE: func(cmd *cobra.Command, args []string) error {
			app := NewContainer(
				viper.GetViper(),
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
							go func() {
								httpErr := http.Serve(listener, h)
								logging.Errorf("http.Serve: %s", httpErr)
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
