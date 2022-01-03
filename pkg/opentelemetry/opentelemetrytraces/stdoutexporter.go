package opentelemetrytraces

import (
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
	"os"
)

func LoadStdoutTracerProvider(f *resourceFactory) (*tracesdk.TracerProvider, error) {

	r, err := f.Make()
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

func StdoutTracerModule() fx.Option {
	return fx.Options(
		fx.Provide(
			LoadStdoutTracerProvider,
		),
		traceSdkExportModule(),
	)
}
