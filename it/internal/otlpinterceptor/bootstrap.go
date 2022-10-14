package otlpinterceptor

import (
	"context"

	. "github.com/onsi/gomega"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/receiver/otlpreceiver"
	"go.opentelemetry.io/collector/service"
	"go.uber.org/zap"
)

var (
	collector    *service.Collector
	collectorErr = make(chan error, 1)
)

func StartCollector() {
	processorFactories, err := component.MakeProcessorFactoryMap(NewFactory())
	Expect(err).WithOffset(1).To(BeNil())

	exporterFactories, err := component.MakeExporterFactoryMap(
		componenttest.NewNopExporterFactory(),
	)

	receiverFactories, err := component.MakeReceiverFactoryMap(
		otlpreceiver.NewFactory(),
	)

	collector, err = service.New(service.CollectorSettings{
		BuildInfo: component.BuildInfo{
			Command:     "otel-interceptor",
			Description: "OpenTelemetry Collector with interception",
			Version:     "1.0.0",
		},
		Factories: component.Factories{
			Processors: processorFactories,
			Exporters:  exporterFactories,
			Receivers:  receiverFactories,
		},
		ConfigProvider:          NewConfigProvider(),
		DisableGracefulShutdown: true,
		SkipSettingGRPCLogger:   true,
		LoggingOptions: []zap.Option{
			zap.Development(),
		},
	})
	Expect(err).WithOffset(1).To(BeNil())

	go func() {
		collectorErr <- collector.Run(context.Background())
	}()
}

func StopCollector() {
	collector.Shutdown()
	Expect(<-collectorErr).To(BeNil())
}
