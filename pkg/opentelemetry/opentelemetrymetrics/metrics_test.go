package opentelemetrymetrics

import (
	"context"
	"fmt"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric"
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
				Exporter: OTLPMetricsExporter,
			},
		},
		{
			name: fmt.Sprintf("otlp-exporter-with-grpc-config"),
			config: MetricsModuleConfig{
				Exporter: OTLPMetricsExporter,
				OTLPConfig: &OTLPMetricsConfig{
					Mode:     opentelemetry.ModeGRPC,
					Endpoint: "remote:8080",
					Insecure: true,
				},
			},
		},
		{
			name: fmt.Sprintf("otlp-exporter-with-http-config"),
			config: MetricsModuleConfig{
				Exporter: OTLPMetricsExporter,
				OTLPConfig: &OTLPMetricsConfig{
					Mode:     opentelemetry.ModeHTTP,
					Endpoint: "remote:8080",
					Insecure: true,
				},
			},
		},
		{
			name: fmt.Sprintf("noop-exporter"),
			config: MetricsModuleConfig{
				Exporter: NoOpMetricsExporter,
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

			ch := make(chan struct{})
			options = append(options, fx.Invoke(func(meter metric.Meter) { // Inject validate the object availability
				close(ch)
			}))

			app := fx.New(options...)
			assert.NoError(t, app.Start(context.Background()))
			defer app.Stop(context.Background())

			select {
			case <-ch:
			default:
				assert.Fail(t, "something went wrong")
			}
		})
	}

}
