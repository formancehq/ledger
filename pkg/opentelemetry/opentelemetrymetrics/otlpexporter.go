package opentelemetrymetrics

import (
	"context"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.uber.org/fx"
)

func LoadOTLPMeterExporter(client otlpmetric.Client) (*otlpmetric.Exporter, error) {
	exp, err := otlpmetric.New(context.Background(), client)
	if err != nil {
		return nil, err
	}
	return exp, nil
}

func LoadOTLPMeterGRPCClient(options ...otlpmetricgrpc.Option) otlpmetric.Client {
	return otlpmetricgrpc.NewClient(options...)
}

func LoadOTLPMeterHTTPClient(options ...otlpmetrichttp.Option) otlpmetric.Client {
	return otlpmetrichttp.NewClient(options...)
}

func OTLPMeterModule() fx.Option {
	return fx.Options(
		fx.Provide(LoadOTLPMeterExporter),
		metricsSdkExportModule(),
		MetricsControllerModule(),
	)
}

const OTLPMeterGRPCOptionsKey = `group:"_otlpMeterGrpcOptions"`

func ProvideOTLPMeterGRPCClientOption(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPMeterGRPCOptionsKey), fx.As(new(otlpmetricgrpc.Option))),
	)
}

func OTLPMeterGRPCClientModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPMeterGRPCClient, fx.ParamTags(OTLPMeterGRPCOptionsKey)),
		),
	)
}

const OTLPMeterHTTPOptionsKey = `group:"_otlpMeterHTTPOptions"`

func ProvideOTLPMeterHTTPClientOption(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPMeterHTTPOptionsKey), fx.As(new(otlpmetrichttp.Option))),
	)
}

func OTLPMeterHTTPClientModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPMeterHTTPClient, fx.ParamTags(OTLPMeterHTTPOptionsKey)),
		),
	)
}
