package opentelemetrymetrics

import (
	"fmt"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/numary/ledger/pkg/opentelemetry/opentelemetrytraces"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric/global"
	"go.uber.org/fx"
	"testing"
)

func TestMetricsModule(t *testing.T) {

	type testCase struct {
		name   string
		config MetricsModuleConfig
	}

	tests := []testCase{
		{
			name: fmt.Sprintf("otlp-exporter"),
			config: MetricsModuleConfig{
				Exporter: opentelemetrytraces.OTLPTracesExporter,
			},
		},
		{
			name: fmt.Sprintf("otlp-exporter-with-config"),
			config: MetricsModuleConfig{
				Exporter: opentelemetrytraces.OTLPTracesExporter,
				OTLPConfig: &OTLPMetricsConfig{
					Mode:     opentelemetry.ModeGRPC,
					Endpoint: "remote:8080",
					Insecure: true,
				},
			},
		},
		{
			name: fmt.Sprintf("noop-exporter"),
			config: MetricsModuleConfig{
				Exporter: opentelemetrytraces.NoOpTracesExporter,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := []fx.Option{MetricsModule(test.config)}
			if !testing.Verbose() {
				options = append(options, fx.NopLogger)
			}
			options = append(options, fx.Provide(func() *testing.T {
				return t
			}))
			assert.NoError(t, fx.ValidateApp(options...))
			assert.NotEmpty(t, global.GetMeterProvider())
		})
	}

}
