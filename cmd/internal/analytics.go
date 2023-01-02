package internal

import (
	"context"
	"time"

	"github.com/Masterminds/semver/v3"
	"github.com/formancehq/go-libs/logging"
	"github.com/numary/ledger/pkg/analytics"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/fx"
)

const (
	// deprecated
	segmentEnabledFlag = "segment-enabled"
	// deprecated
	segmentWriteKeyFlag = "segment-write-key"
	// deprecated
	segmentApplicationIdFlag = "segment-application-id"
	// deprecated
	segmentHeartbeatIntervalFlag = "segment-heartbeat-interval"

	telemetryEnabledFlag           = "telemetry-enabled"
	telemetryWriteKeyFlag          = "telemetry-write-key"
	telemetryApplicationIdFlag     = "telemetry-application-id"
	telemetryHeartbeatIntervalFlag = "telemetry-heartbeat-interval"
)

func InitAnalyticsFlags(cmd *cobra.Command, defaultWriteKey string) {
	cmd.PersistentFlags().Bool(segmentEnabledFlag, false, "Is segment enabled")
	cmd.PersistentFlags().String(segmentApplicationIdFlag, "", "Segment application id")
	cmd.PersistentFlags().String(segmentWriteKeyFlag, defaultWriteKey, "Segment write key")
	cmd.PersistentFlags().Duration(segmentHeartbeatIntervalFlag, 4*time.Hour, "Segment heartbeat interval")
	cmd.PersistentFlags().Bool(telemetryEnabledFlag, true, "Is telemetry enabled")
	cmd.PersistentFlags().String(telemetryApplicationIdFlag, "", "telemetry application id")
	cmd.PersistentFlags().String(telemetryWriteKeyFlag, defaultWriteKey, "telemetry write key")
	cmd.PersistentFlags().Duration(telemetryHeartbeatIntervalFlag, 4*time.Hour, "telemetry heartbeat interval")
}

func NewAnalyticsModule(v *viper.Viper, version string) fx.Option {
	if v.GetBool(telemetryEnabledFlag) || v.GetBool(segmentEnabledFlag) {
		applicationId := viper.GetString(telemetryApplicationIdFlag)
		if applicationId == "" {
			applicationId = viper.GetString(segmentApplicationIdFlag)
		}
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
		if writeKey == "" {
			writeKey = viper.GetString(segmentWriteKeyFlag)
		}
		interval := viper.GetDuration(telemetryHeartbeatIntervalFlag)
		if interval == 0 {
			interval = viper.GetDuration(segmentHeartbeatIntervalFlag)
		}
		if writeKey == "" {
			logging.GetLogger(context.Background()).Infof("telemetry enabled but no write key provided")
		} else if interval == 0 {
			logging.GetLogger(context.Background()).Error("telemetry heartbeat interval is 0")
		} else {
			_, err := semver.NewVersion(version)
			if err != nil {
				logging.GetLogger(context.Background()).Infof("telemetry enabled but version '%s' is not semver, skip", version)
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
