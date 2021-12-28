package opentelemetry

import (
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
	"os"
)

func LoadStdoutTracerProvider(serviceName, version string) (*tracesdk.TracerProvider, error) {

	r, err := newResource(serviceName, version)
	if err != nil {
		return nil, err
	}
	exp, err := stdouttrace.New(
		stdouttrace.WithWriter(os.Stdout),
	)
	if err != nil {
		return nil, err
	}
	tp := tracesdk.NewTracerProvider(
		tracesdk.WithBatcher(exp),
		tracesdk.WithResource(r),
	)
	return tp, nil
}

func StdoutModule() fx.Option {
	return fx.Options(
		fx.Provide(
			fx.Annotate(LoadStdoutTracerProvider, fx.ParamTags(
				ServiceNameKey,
				ServiceVersionKey,
			)),
		),
		traceSdkExportModule(),
	)
}
