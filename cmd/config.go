package cmd

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/mitchellh/mapstructure"
	"github.com/robfig/cron/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"

	"github.com/formancehq/ledger/internal/controller/ledger"
)

type commonConfig struct {
	NumscriptInterpreter        bool                         `mapstructure:"experimental-numscript-interpreter"`
	NumscriptInterpreterFlags   []string                     `mapstructure:"experimental-numscript-interpreter-flags"`
	ExperimentalFeaturesEnabled bool                         `mapstructure:"experimental-features"`
	ExperimentalExporters       bool                         `mapstructure:"experimental-exporters"`
	ExperimentalGlobalExporter  string                       `mapstructure:"experimental-global-exporter"`
	GlobalExporterReset         bool                         `mapstructure:"global-exporter-reset"`
	SemconvMetricsNames         bool                         `mapstructure:"semconv-metrics-names"`
	SchemaEnforcementMode       ledger.SchemaEnforcementMode `mapstructure:"schema-enforcement-mode"`
}

func decodeCronSchedule(sourceType, destType reflect.Type, value any) (any, error) {
	if sourceType.Kind() != reflect.String {
		return value, nil
	}
	if destType != reflect.TypeOf((*cron.Schedule)(nil)).Elem() {
		return value, nil
	}

	parser := cron.NewParser(cron.Second | cron.Minute | cron.Hour | cron.Dom | cron.Month | cron.Dow | cron.Descriptor)
	schedule, err := parser.Parse(value.(string))
	if err != nil {
		return nil, fmt.Errorf("parsing cron schedule: %w", err)
	}

	return schedule, nil
}

func parseGlobalExporter(value string) (string, json.RawMessage, error) {
	idx := strings.Index(value, ":")
	if idx == -1 {
		return "", nil, fmt.Errorf("invalid experimental-global-exporter format, expected <driver>:<config>")
	}
	driverName := value[:idx]
	configStr := value[idx+1:]

	return driverName, json.RawMessage(configStr), nil
}

func LoadConfig[V any](cmd *cobra.Command) (*V, error) {
	v := viper.New()
	if err := v.BindPFlags(cmd.Flags()); err != nil {
		return nil, fmt.Errorf("binding flags: %w", err)
	}

	var cfg V
	if err := v.Unmarshal(&cfg,
		viper.DecodeHook(mapstructure.ComposeDecodeHookFunc(
			decodeCronSchedule,
			mapstructure.StringToTimeDurationHookFunc(),
		)),
	); err != nil {
		return nil, fmt.Errorf("unmarshalling config: %w", err)
	}

	return &cfg, nil
}
