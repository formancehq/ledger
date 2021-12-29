package opentelemetrytraces

import (
	"context"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

func LoadOTLPTracerProvider(serviceName string, version string, client otlptrace.Client) (*tracesdk.TracerProvider, error) {
	r, err := newResource(serviceName, version)
	if err != nil {
		return nil, err
	}

	exp, err := otlptrace.New(context.Background(), client)
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(r),
	)
	return tp, nil
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
			fx.Annotate(LoadOTLPTracerProvider, fx.ParamTags(
				ServiceNameKey,
				ServiceVersionKey,
			)),
		),
		traceSdkExportModule(),
	)
}

const OTLPTracerGRPCOptionsKey = `group:"_otlpTracerGrpcOptions"`

func ProvideOTLPTracerGRPCClientOption(provider interface{}) fx.Option {
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

func ProvideOTLPTracerHTTPClientOption(provider interface{}) fx.Option {
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
