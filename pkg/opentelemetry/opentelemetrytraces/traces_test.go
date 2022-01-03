package opentelemetrytraces

import (
	"context"
	"fmt"
	"github.com/numary/ledger/pkg/opentelemetry"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"testing"
)

func TestTracesModule(t *testing.T) {

	type testCase struct {
		name   string
		config ModuleConfig
	}

	tests := []testCase{
		{
			name: fmt.Sprintf("otlp-exporter"),
			config: ModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    OTLPExporter,
			},
		},
		{
			name: fmt.Sprintf("otlp-exporter-with-grpc-config"),
			config: ModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    OTLPExporter,
				OTLPConfig: &OTLPConfig{
					Mode:     opentelemetry.ModeGRPC,
					Endpoint: "remote:8080",
					Insecure: true,
				},
			},
		},
		{
			name: fmt.Sprintf("otlp-exporter-with-http-config"),
			config: ModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    OTLPExporter,
				OTLPConfig: &OTLPConfig{
					Mode:     opentelemetry.ModeHTTP,
					Endpoint: "remote:8080",
					Insecure: true,
				},
			},
		},
		{
			name: fmt.Sprintf("jaeger-exporter"),
			config: ModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    JaegerExporter,
			},
		},
		{
			name: fmt.Sprintf("jaeger-exporter-with-config"),
			config: ModuleConfig{
				ServiceName:  "testing",
				Version:      "latest",
				Exporter:     JaegerExporter,
				JaegerConfig: &JaegerConfig{},
			},
		},
		{
			name: fmt.Sprintf("noop-exporter"),
			config: ModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    NoOpExporter,
			},
		},
		{
			name: fmt.Sprintf("stdout-exporter"),
			config: ModuleConfig{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    StdoutExporter,
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

			ch := make(chan struct{})
			options = append(options, fx.Invoke(func(f *resourceFactory) { // Inject validate the object availability
				assert.Len(t, f.attributes, 2)
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
