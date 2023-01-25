package analytics

import (
	"context"
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/segmentio/analytics-go"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

type roundTripperFn func(req *http.Request) (*http.Response, error)

func (fn roundTripperFn) RoundTrip(req *http.Request) (*http.Response, error) {
	return fn(req)
}

func TestAnalyticsModule(t *testing.T) {
	v := viper.GetViper()
	v.Set(telemetryEnabledFlag, true)
	v.Set(telemetryWriteKeyFlag, "XXX")
	v.Set(telemetryApplicationIdFlag, "appId")
	v.Set(telemetryHeartbeatIntervalFlag, 10*time.Second)

	handled := make(chan *analytics.Track, 1)

	module := NewAnalyticsModule(v, "1.0.0", true)
	app := fx.New(
		module,
		fx.NopLogger,
		fx.Supply(fx.Annotate(PropertiesEnricherFn(func(ctx context.Context, p analytics.Properties) error {
			p.Set("additionalProperty", "test")
			return nil
		}), fx.As(new(PropertiesEnricher)), fx.ResultTags(FXTagPropertiesEnrichers))),
		fx.Replace(analytics.Config{
			BatchSize: 1,
			Transport: roundTripperFn(func(req *http.Request) (*http.Response, error) {
				select {
				case <-handled:
					// Nothing to do, the chan has already been closed
				default:
					type batch struct {
						Messages []*analytics.Track `json:"batch"`
					}
					b := batch{}
					if err := json.NewDecoder(req.Body).Decode(&b); err != nil {
						panic(err)
					}
					handled <- b.Messages[0]
					close(handled)
				}
				return &http.Response{
					StatusCode: http.StatusOK,
				}, nil
			}),
		}))
	require.NoError(t, app.Start(context.Background()))
	defer func() {
		require.NoError(t, app.Stop(context.Background()))
	}()

	select {
	case <-time.After(time.Second):
		require.Fail(t, "Timeout waiting first stats from analytics module")
	case track := <-handled:
		require.Equal(t, "test", track.Properties["additionalProperty"])
	}

}

func TestAnalyticsModuleDisabled(t *testing.T) {
	v := viper.GetViper()
	v.Set(telemetryEnabledFlag, false)

	module := NewAnalyticsModule(v, "1.0.0", true)
	app := fx.New(module, fx.NopLogger)
	require.NoError(t, app.Start(context.Background()))
	require.NoError(t, app.Stop(context.Background()))
}
