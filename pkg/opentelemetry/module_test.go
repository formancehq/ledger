package opentelemetry

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"go.uber.org/fx"
	"testing"
)

func TestModule(t *testing.T) {

	type testCase struct {
		name   string
		config Config
	}

	tests := []testCase{
		{
			name: fmt.Sprintf("otlp-exporter"),
			config: Config{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    OTLPExporter,
			},
		},
		{
			name: fmt.Sprintf("jaeger-exporter"),
			config: Config{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    JaegerExporter,
			},
		},
		{
			name: fmt.Sprintf("noop-exporter"),
			config: Config{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    NoOpExporter,
			},
		},
		{
			name: fmt.Sprintf("stdout-exporter"),
			config: Config{
				ServiceName: "testing",
				Version:     "latest",
				Exporter:    StdoutExporter,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := []fx.Option{Module(test.config)}
			if !testing.Verbose() {
				options = append(options, fx.NopLogger)
			}
			assert.NoError(t, fx.ValidateApp(options...))
		})
	}

}
