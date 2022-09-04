package otlpinterceptor

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/config/confighttp"
	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/telemetry"
	"go.uber.org/zap/zapcore"
)

const (
	HTTPPort = 4317
)

type ConfigProvider struct {
	ch chan error
}

func (c *ConfigProvider) Get(ctx context.Context, factories component.Factories) (*service.Config, error) {
	exporterSettings := config.NewExporterSettings(config.NewComponentID("nop"))
	return &service.Config{
		Receivers: map[config.ComponentID]config.Receiver{
			config.NewComponentID("otlp"): &otlpreceiver.Config{
				ReceiverSettings: config.NewReceiverSettings(config.NewComponentID("otlp")),
				Protocols: otlpreceiver.Protocols{
					HTTP: &confighttp.HTTPServerSettings{
						Endpoint: fmt.Sprintf("0.0.0.0:%d", HTTPPort),
					},
				},
			},
		},
		Exporters: map[config.ComponentID]config.Exporter{
			config.NewComponentID("nop"): &exporterSettings,
		},
		Processors: map[config.ComponentID]config.Processor{
			config.NewComponentID("interceptor"): &Config{},
		},
		Service: service.ConfigService{
			Pipelines: map[config.ComponentID]*service.ConfigServicePipeline{
				config.NewComponentID("traces"): {
					Receivers: []config.ComponentID{
						config.NewComponentID("otlp"),
					},
					Processors: []config.ComponentID{
						config.NewComponentID("interceptor"),
					},
					Exporters: []config.ComponentID{
						config.NewComponentID("nop"),
					},
				},
			},
			Telemetry: telemetry.Config{
				Logs: telemetry.LogsConfig{
					Development: true,
					Encoding:    "console",
					Level:       zapcore.InfoLevel,
					//OutputPaths:      []string{"stderr"},
					//ErrorOutputPaths: []string{"stderr"},
				},
				Metrics: telemetry.MetricsConfig{
					Level:   configtelemetry.LevelBasic,
					Address: ":9093",
				},
			},
		},
	}, nil
}

func (c *ConfigProvider) Watch() <-chan error {
	return c.ch
}

func (c *ConfigProvider) Shutdown(ctx context.Context) error {
	return nil
}

var _ service.ConfigProvider = &ConfigProvider{}

func NewConfigProvider() *ConfigProvider {
	return &ConfigProvider{ch: make(chan error, 1)}
}
