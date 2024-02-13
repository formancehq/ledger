package internal

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
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

func TestAnalyticsModuleDisabled(t *testing.T) {
	v := viper.GetViper()
	v.Set(telemetryEnabledFlag, false)

	module := NewAnalyticsModule("1.0.0")
	app := fx.New(module)
	require.NoError(t, app.Start(context.Background()))
	require.NoError(t, app.Stop(context.Background()))
}
