package opentelemetry

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

func LoadOTLPGRPCClient(options ...otlptracegrpc.Option) otlptrace.Client {
	return otlptracegrpc.NewClient(options...)
}

func LoadOTLPHTTPClient(options ...otlptracehttp.Option) otlptrace.Client {
	return otlptracehttp.NewClient(options...)
}

func OTLPModule() fx.Option {
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

const OTLPGRPCOptionsKey = `group:"_otlpGrpcOptions"`

func ProvideOTLPGRPCClientOption(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPGRPCOptionsKey), fx.As(new(otlptracegrpc.Option))),
	)
}

func OTLPGRPCClientModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPGRPCClient, fx.ParamTags(OTLPGRPCOptionsKey)),
		),
	)
}

const OTLPHTTPOptionsKey = `group:"_otlpHTTPOptions"`

func ProvideOTLPHTTPClientOption(provider interface{}) fx.Option {
	return fx.Provide(
		fx.Annotate(provider, fx.ResultTags(OTLPHTTPOptionsKey), fx.As(new(otlptracehttp.Option))),
	)
}

func OTLPHTTPClientModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadOTLPHTTPClient, fx.ParamTags(OTLPHTTPOptionsKey)),
		),
	)
}
