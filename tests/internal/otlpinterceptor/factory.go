package otlpinterceptor

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
)

const (
	// The value of "type" key in configuration.
	typeStr = "interceptor"
)

type Config struct {
	config.ProcessorSettings `mapstructure:",squash"`
}

// NewFactory creates a factory for the routing processor.
func NewFactory() component.ProcessorFactory {
	return component.NewProcessorFactory(
		typeStr,
		createDefaultConfig,
		component.WithTracesProcessor(
			createTraceProcessor,
			component.StabilityLevelInDevelopment,
		),
	)
}

func createDefaultConfig() config.Processor {
	return &Config{
		ProcessorSettings: config.NewProcessorSettings(
			config.NewComponentIDWithName(typeStr, typeStr),
		),
	}
}

func createTraceProcessor(
	_ context.Context,
	_ component.ProcessorCreateSettings,
	_ config.Processor,
	nextConsumer consumer.Traces) (component.TracesProcessor, error) {

	return GlobalInterceptor, nil
}
