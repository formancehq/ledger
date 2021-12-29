package opentelemetrytraces

import (
	"fmt"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"testing"
)

func TestTracesModule(t *testing.T) {

	type testCase struct {
		name   string
		config TracesModuleConfig
	}

	tests := []testCase{
		{
			name: fmt.Sprintf("otlp-exporter"),
			config: TracesModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    OTLPTracesExporter,
			},
		},
		{
			name: fmt.Sprintf("otlp-exporter-with-config"),
			config: TracesModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    OTLPTracesExporter,
				OTLPConfig: &OTLPTracesConfig{
					Mode:     opentelemetry.ModeGRPC,
					Endpoint: "remote:8080",
					Insecure: true,
				},
			},
		},

		{
			name: fmt.Sprintf("jaeger-exporter"),
			config: TracesModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    JaegerTracesExporter,
			},
		},
		{
			name: fmt.Sprintf("noop-exporter"),
			config: TracesModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    NoOpTracesExporter,
			},
		},
		{
			name: fmt.Sprintf("stdout-exporter"),
			config: TracesModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    StdoutTracesExporter,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := []fx.Option{TracesModule(test.config)}
			if !testing.Verbose() {
				options = append(options, fx.NopLogger)
			}
			options = append(options, fx.Provide(func() *testing.T {
				return t
			}))
			assert.NoError(t, fx.ValidateApp(options...))
		})
	}

}
