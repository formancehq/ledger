package analytics

import (
	"context"
	"time"

	"github.com/google/uuid"
	"github.com/segmentio/analytics-go"
	"go.uber.org/fx"
)

const (
	FXTagPropertiesEnrichers = `group:"enrichers"`
)

func NewHeartbeatModule(version, writeKey string, interval time.Duration) fx.Option {
	defaultAppId := uuid.NewString()
	return fx.Options(
		fx.Supply(analytics.Config{}), // Provide empty config to be able to replace (use fx.Replace) if necessary
		fx.Provide(func(cfg analytics.Config) (analytics.Client, error) {
			return analytics.NewWithConfig(writeKey, cfg)
		}),
		fx.Provide(fx.Annotate(func(client analytics.Client, provider AppIdProvider, enrichers []PropertiesEnricher) *heartbeat {
			return newHeartbeat(provider, client, version, interval, enrichers...)
		}, fx.ParamTags("", "", FXTagPropertiesEnrichers))),
		fx.Provide(func() AppIdProvider {
			return AppIdProviderFn(func(ctx context.Context) (string, error) {
				return defaultAppId, nil
			})
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
