package internal

import (
	"context"
	"net/http"
	"reflect"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/ledgertesting"
	"github.com/formancehq/ledger/pkg/storage"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
	"gopkg.in/segmentio/analytics-go.v3"
)

func TestAnalyticsFlags(t *testing.T) {
	type testCase struct {
		name          string
		key           string
		envValue      string
		viperMethod   interface{}
		expectedValue interface{}
	}

	for _, testCase := range []testCase{
		{
			name:          "using telemetry enabled flag",
			key:           telemetryEnabledFlag,
			envValue:      "true",
			viperMethod:   (*viper.Viper).GetBool,
			expectedValue: true,
		},
		{
			name:          "using telemetry write key flag",
			key:           telemetryWriteKeyFlag,
			envValue:      "foo:bar",
			viperMethod:   (*viper.Viper).GetString,
			expectedValue: "foo:bar",
		},
		{
			name:          "using telemetry heartbeat interval flag",
			key:           telemetryHeartbeatIntervalFlag,
			envValue:      "10s",
			viperMethod:   (*viper.Viper).GetDuration,
			expectedValue: 10 * time.Second,
		},
		{
			name:          "using telemetry application id flag",
			key:           telemetryApplicationIdFlag,
			envValue:      "foo:bar",
			viperMethod:   (*viper.Viper).GetString,
			expectedValue: "foo:bar",
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			v := viper.GetViper()
			cmd := &cobra.Command{
				Run: func(cmd *cobra.Command, args []string) {
					ret := reflect.ValueOf(testCase.viperMethod).Call([]reflect.Value{
						reflect.ValueOf(v),
						reflect.ValueOf(testCase.key),
					})
					require.Len(t, ret, 1)

					rValue := ret[0].Interface()
					require.Equal(t, testCase.expectedValue, rValue)
				},
			}
			BindEnv(v)

			restoreEnvVar := setEnvVar(testCase.key, testCase.envValue)
			defer restoreEnvVar()

			require.NoError(t, v.BindPFlags(cmd.PersistentFlags()))
			require.NoError(t, cmd.Execute())
		})
	}
}

func TestAnalyticsModule(t *testing.T) {
	v := viper.GetViper()
	v.Set(telemetryEnabledFlag, true)
	v.Set(telemetryWriteKeyFlag, "XXX")
	v.Set(telemetryApplicationIdFlag, "appId")
	v.Set(telemetryHeartbeatIntervalFlag, 10*time.Second)

	handled := make(chan struct{})

	module := NewAnalyticsModule(v, "1.0.0")
	app := fx.New(
		module,
		fx.Provide(func(lc fx.Lifecycle) (storage.Driver[ledger.Store], error) {
			driver, stopFn, err := ledgertesting.StorageDriver()
			if err != nil {
				return nil, err
			}
			lc.Append(fx.Hook{
				OnStart: driver.Initialize,
				OnStop: func(ctx context.Context) error {
					stopFn()
					return driver.Close(ctx)
				},
			})
			return sqlstorage.NewLedgerStorageDriverFromRawDriver(driver), nil
		}),
		fx.Replace(analytics.Config{
			BatchSize: 1,
			Transport: roundTripperFn(func(req *http.Request) (*http.Response, error) {
				select {
				case <-handled:
					// Nothing to do, the chan has already been closed
				default:
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
	case <-handled:
	}

}

func TestAnalyticsModuleDisabled(t *testing.T) {
	v := viper.GetViper()
	v.Set(telemetryEnabledFlag, false)

	module := NewAnalyticsModule(v, "1.0.0")
	app := fx.New(module)
	require.NoError(t, app.Start(context.Background()))
	require.NoError(t, app.Stop(context.Background()))
}
