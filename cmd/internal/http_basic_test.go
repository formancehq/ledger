package internal

import (
	"fmt"
	"os"
	"reflect"
	"strings"
	"testing"

	"github.com/numary/go-libs/sharedauth"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

func withPrefix(flag string) string {
	return strings.ToUpper(fmt.Sprintf("%s_%s", envPrefix, EnvVarReplacer.Replace(flag)))
}

func setEnvVar(key, value string) func() {
	prefixedFlag := withPrefix(key)
	oldEnv := os.Getenv(prefixedFlag)
	os.Setenv(prefixedFlag, value)
	return func() {
		os.Setenv(prefixedFlag, oldEnv)
	}
}

func TestViperEnvBinding(t *testing.T) {

	type testCase struct {
		name          string
		key           string
		envValue      string
		viperMethod   interface{}
		expectedValue interface{}
	}

	for _, testCase := range []testCase{
		{
			name:          "using deprecated credentials flag",
			key:           serverHttpBasicAuthFlag,
			envValue:      "foo:bar",
			viperMethod:   (*viper.Viper).GetString,
			expectedValue: "foo:bar",
		},
		{
			name:          "using credentials flag",
			key:           authBasicCredentialsFlag,
			envValue:      "foo:bar",
			viperMethod:   (*viper.Viper).GetStringSlice,
			expectedValue: []string{"foo:bar"},
		},
		{
			name:          "using http basic enabled flags",
			key:           authBasicEnabledFlag,
			envValue:      "true",
			viperMethod:   (*viper.Viper).GetBool,
			expectedValue: true,
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
			InitHTTPBasicFlags(cmd)
			BindEnv(v)

			restoreEnvVar := setEnvVar(testCase.key, testCase.envValue)
			defer restoreEnvVar()

			require.NoError(t, v.BindPFlags(cmd.PersistentFlags()))

			require.NoError(t, cmd.Execute())
		})
	}
}

func TestHTTPBasicAuthMethod(t *testing.T) {

	type testCase struct {
		name                    string
		args                    []string
		expectedBasicAuthMethod bool
	}

	for _, testCase := range []testCase{
		{
			name:                    "no flag defined",
			args:                    []string{},
			expectedBasicAuthMethod: false,
		},
		{
			name: "with latest credentials flag",
			args: []string{
				fmt.Sprintf("--%s=%s", authBasicCredentialsFlag, "foo:bar"),
			},
			expectedBasicAuthMethod: true,
		},
		{
			name: "with deprecated credentials flag",
			args: []string{
				fmt.Sprintf("--%s=%s", serverHttpBasicAuthFlag, "foo:bar"),
			},
			expectedBasicAuthMethod: true,
		},
		{
			name: "with enabled flag set to false",
			args: []string{
				fmt.Sprintf("--%s=%s", serverHttpBasicAuthFlag, "foo:bar"),
				fmt.Sprintf("--%s=false", authBasicEnabledFlag),
			},
			expectedBasicAuthMethod: false,
		},
		{
			name: "with enabled flag set to true",
			args: []string{
				fmt.Sprintf("--%s=%s", serverHttpBasicAuthFlag, "foo:bar"),
				fmt.Sprintf("--%s=true", authBasicEnabledFlag),
			},
			expectedBasicAuthMethod: true,
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			var method sharedauth.Method
			cmd := &cobra.Command{
				RunE: func(cmd *cobra.Command, args []string) error {
					method = HTTPBasicAuthMethod(viper.GetViper())
					return nil
				},
			}
			InitHTTPBasicFlags(cmd)
			require.NoError(t, viper.BindPFlags(cmd.PersistentFlags()))

			cmd.SetArgs(testCase.args)

			require.NoError(t, cmd.Execute())
			if testCase.expectedBasicAuthMethod {
				require.NotNil(t, method)
			} else {
				require.Nil(t, method)
			}
		})
	}
}
