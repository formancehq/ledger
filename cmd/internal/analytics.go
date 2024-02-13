package internal

import (
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/formancehq/ledger/internal/analytics"
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

func NewAnalyticsModule(version string) fx.Option {
	if viper.GetBool(telemetryEnabledFlag) {
		applicationID := viper.GetString(telemetryApplicationIdFlag)
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
				return analytics.NewHeartbeatModule(version, writeKey, applicationID, interval)
			}
		}
	}
	return fx.Options()
}
