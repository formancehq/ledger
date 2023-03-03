package internal

import (
	"context"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/formancehq/ledger/pkg/analytics"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const (
	telemetryEnabledFlag           = "telemetry-enabled"
	telemetryWriteKeyFlag          = "telemetry-write-key"
	telemetryApplicationIdFlag     = "telemetry-application-id"
	telemetryHeartbeatIntervalFlag = "telemetry-heartbeat-interval"
)

func InitAnalyticsFlags(cmd *cobra.Command, defaultWriteKey string) {
	cmd.PersistentFlags().Bool(telemetryEnabledFlag, true, "Is telemetry enabled")
	cmd.PersistentFlags().String(telemetryApplicationIdFlag, "", "telemetry application id")
	cmd.PersistentFlags().String(telemetryWriteKeyFlag, defaultWriteKey, "telemetry write key")
	cmd.PersistentFlags().Duration(telemetryHeartbeatIntervalFlag, 4*time.Hour, "telemetry heartbeat interval")
}

func NewAnalyticsModule(v *viper.Viper, version string) fx.Option {
	if v.GetBool(telemetryEnabledFlag) {
		applicationId := viper.GetString(telemetryApplicationIdFlag)
		var appIdProviderModule fx.Option
		if applicationId == "" {
			appIdProviderModule = fx.Provide(analytics.FromStorageAppIdProvider)
		} else {
			appIdProviderModule = fx.Provide(func() analytics.AppIdProvider {
				return analytics.AppIdProviderFn(func(ctx context.Context) (string, error) {
					return applicationId, nil
				})
			})
		}
		writeKey := viper.GetString(telemetryWriteKeyFlag)
		interval := viper.GetDuration(telemetryHeartbeatIntervalFlag)
		if writeKey == "" {
			return fx.Invoke(func(l logging.Logger) {
				l.Infof("telemetry enabled but no write key provided")
			})
		} else if interval == 0 {
			return fx.Invoke(func(l logging.Logger) {
				l.Error("telemetry heartbeat interval is 0")
			})
		} else {
			_, err := semver.NewVersion(version)
			if err != nil {
				return fx.Invoke(func(l logging.Logger) {
					l.Infof("telemetry enabled but version '%s' is not semver, skip", version)
				})
			} else {
				return fx.Options(
					appIdProviderModule,
					analytics.NewHeartbeatModule(version, writeKey, interval),
				)
			}
		}
	}
	return fx.Options()
}
