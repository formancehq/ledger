package analytics

import (
	"time"

	"github.com/coreos/go-semver/semver"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const (
	// deprecated
	segmentEnabledFlag = "segment-enabled"
	// deprecated
	segmentWriteKeyFlag = "segment-write-flagKey"
	// deprecated
	segmentApplicationIdFlag = "segment-application-id"
	// deprecated
	segmentHeartbeatIntervalFlag = "segment-heartbeat-interval"

	telemetryEnabledFlag           = "telemetry-enabled"
	telemetryWriteKeyFlag          = "telemetry-write-flagKey"
	telemetryApplicationIdFlag     = "telemetry-application-id"
	telemetryHeartbeatIntervalFlag = "telemetry-heartbeat-interval"
)

func InitAnalyticsFlags(cmd *cobra.Command, defaultWriteKey string, useDeprecatedFlags bool) {
	if useDeprecatedFlags {
		cmd.PersistentFlags().Bool(segmentEnabledFlag, false, "Is segment enabled")
		cmd.PersistentFlags().String(segmentApplicationIdFlag, "", "Segment application id")
		cmd.PersistentFlags().String(segmentWriteKeyFlag, defaultWriteKey, "Segment write flagKey")
		cmd.PersistentFlags().Duration(segmentHeartbeatIntervalFlag, 4*time.Hour, "Segment heartbeat interval")
	}
	cmd.PersistentFlags().Bool(telemetryEnabledFlag, true, "Is telemetry enabled")
	cmd.PersistentFlags().String(telemetryApplicationIdFlag, "", "telemetry application id")
	cmd.PersistentFlags().String(telemetryWriteKeyFlag, defaultWriteKey, "telemetry write flagKey")
	cmd.PersistentFlags().Duration(telemetryHeartbeatIntervalFlag, 4*time.Hour, "telemetry heartbeat interval")
}

func NewAnalyticsModule(logger logging.Logger, v *viper.Viper, version string, useDeprecatedFlags bool) fx.Option {
	if v.GetBool(telemetryEnabledFlag) || (useDeprecatedFlags && v.GetBool(segmentEnabledFlag)) {

		writeKey := viper.GetString(telemetryWriteKeyFlag)
		if writeKey == "" && useDeprecatedFlags {
			writeKey = viper.GetString(segmentWriteKeyFlag)
		}
		interval := viper.GetDuration(telemetryHeartbeatIntervalFlag)
		if interval == 0 && useDeprecatedFlags {
			interval = viper.GetDuration(segmentHeartbeatIntervalFlag)
		}
		if writeKey == "" {
			logger.Infof("telemetry enabled but no write flagKey provided")
		} else if interval == 0 {
			logger.Error("telemetry heartbeat interval is 0")
		} else {
			_, err := semver.NewVersion(version)
			if err != nil {
				logger.Infof("telemetry enabled but version '%s' is not semver, skip", version)
			} else {
				return NewHeartbeatModule(version, writeKey, interval)
			}
		}
	}
	return fx.Options()
}
