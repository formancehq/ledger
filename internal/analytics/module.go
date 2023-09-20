package analytics

import (
	"context"
	"time"

	"github.com/formancehq/ledger/internal/storage/driver"
	"go.uber.org/fx"
	"gopkg.in/segmentio/analytics-go.v3"
)

func NewHeartbeatModule(version, writeKey, appID string, interval time.Duration) fx.Option {
	return fx.Options(
		fx.Supply(analytics.Config{}), // Provide empty config to be able to replace (use fx.Replace) if necessary
		fx.Provide(func(cfg analytics.Config) (analytics.Client, error) {
			return analytics.NewWithConfig(writeKey, cfg)
		}),
		fx.Provide(func(client analytics.Client, backend Backend) *heartbeat {
			return newHeartbeat(backend, client, version, interval)
		}),
		fx.Provide(func(driver *driver.Driver) Backend {
			return newDefaultBackend(driver, appID)
		}),
		fx.Invoke(func(m *heartbeat, lc fx.Lifecycle) {
			lc.Append(fx.Hook{
				OnStart: func(ctx context.Context) error {
					go func() {
						err := m.Run(context.Background())
						if err != nil {
							panic(err)
						}
					}()
					return nil
				},
				OnStop: func(ctx context.Context) error {
					return m.Stop(ctx)
				},
			})
		}),
		fx.Invoke(func(lc fx.Lifecycle, client analytics.Client) {
			lc.Append(fx.Hook{
				OnStop: func(ctx context.Context) error {
					return client.Close()
				},
			})
		}),
	)
}
