package analytics

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func TestAnalyticsFlags(t *testing.T) {
	type testCase struct {
		name          string
		flagKey       string
		flagValue     string
		viperMethod   interface{}
		expectedValue interface{}
	}

	for _, testCase := range []testCase{
		{
			name:          "using deprecated segment enabled flag",
			flagKey:       segmentEnabledFlag,
			flagValue:     "true",
			viperMethod:   (*viper.Viper).GetBool,
			expectedValue: true,
		},
		{
			name:          "using deprecated segment write flagKey flag",
			flagKey:       segmentWriteKeyFlag,
			flagValue:     "foo:bar",
			viperMethod:   (*viper.Viper).GetString,
			expectedValue: "foo:bar",
		},
		{
			name:          "using deprecated segment heartbeat interval flag",
			flagKey:       segmentHeartbeatIntervalFlag,
			flagValue:     "10s",
			viperMethod:   (*viper.Viper).GetDuration,
			expectedValue: 10 * time.Second,
		},
		{
			name:          "using deprecated segment application id flag",
			flagKey:       segmentApplicationIdFlag,
			flagValue:     "foo:bar",
			viperMethod:   (*viper.Viper).GetString,
			expectedValue: "foo:bar",
		},
		{
			name:          "using telemetry enabled flag",
			flagKey:       telemetryEnabledFlag,
			flagValue:     "true",
			viperMethod:   (*viper.Viper).GetBool,
			expectedValue: true,
		},
		{
			name:          "using telemetry write flagKey flag",
			flagKey:       telemetryWriteKeyFlag,
			flagValue:     "foo:bar",
			viperMethod:   (*viper.Viper).GetString,
			expectedValue: "foo:bar",
		},
		{
			name:          "using telemetry heartbeat interval flag",
			flagKey:       telemetryHeartbeatIntervalFlag,
			flagValue:     "10s",
			viperMethod:   (*viper.Viper).GetDuration,
			expectedValue: 10 * time.Second,
		},
		{
			name:          "using telemetry application id flag",
			flagKey:       telemetryApplicationIdFlag,
			flagValue:     "foo:bar",
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
						reflect.ValueOf(testCase.flagKey),
					})
					require.Len(t, ret, 1)

					rValue := ret[0].Interface()
					require.Equal(t, testCase.expectedValue, rValue)
				},
			}
			InitAnalyticsFlags(cmd, "xxx", true)

			cmd.SetArgs([]string{fmt.Sprintf("--%s", testCase.flagKey), testCase.flagValue})

			require.NoError(t, v.BindPFlags(cmd.PersistentFlags()))
			require.NoError(t, cmd.Execute())
		})
	}
}
