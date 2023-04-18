package otlptraces

import (
	"context"

	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

func LoadOTLPTracerProvider(client otlptrace.Client) (*otlptrace.Exporter, error) {
	return otlptrace.New(context.Background(), client)
}

func LoadOTLPTracerGRPCClient(options ...otlptracegrpc.Option) otlptrace.Client {
	return otlptracegrpc.NewClient(options...)
}

func LoadOTLPTracerHTTPClient(options ...otlptracehttp.Option) otlptrace.Client {
	return otlptracehttp.NewClient(options...)
}

func OTLPTracerModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPTracerProvider, fx.As(new(trace.SpanExporter))),
		),
	)
}

const OTLPTracerGRPCOptionsKey = `group:"_otlpTracerGrpcOptions"`

func ProvideOTLPTracerGRPCClientOption(provider any) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPTracerGRPCOptionsKey), fx.As(new(otlptracegrpc.Option))),
	)
}

func OTLPTracerGRPCClientModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPTracerGRPCClient, fx.ParamTags(OTLPTracerGRPCOptionsKey)),
		),
	)
}

const OTLPTracerHTTPOptionsKey = `group:"_otlpTracerHTTPOptions"`

func ProvideOTLPTracerHTTPClientOption(provider any) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPTracerHTTPOptionsKey), fx.As(new(otlptracehttp.Option))),
	)
}

func OTLPTracerHTTPClientModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPTracerHTTPClient, fx.ParamTags(OTLPTracerHTTPOptionsKey)),
		),
	)
}
