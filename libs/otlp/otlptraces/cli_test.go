package otlptraces

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/formancehq/stack/libs/go-libs/otlp"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/jaeger"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	tracesdk "go.opentelemetry.io/otel/sdk/trace"
	"go.uber.org/fx"
)

func TestOTLPTracesModule(t *testing.T) {
	type testCase struct {
		name                 string
		args                 []string
		expectedSpanExporter tracesdk.SpanExporter
	}

	for _, testCase := range []testCase{
		{
			name: "jaeger",
			args: []string{
				fmt.Sprintf("--%s", OtelTracesFlag),
				fmt.Sprintf("--%s=%s", OtelTracesExporterFlag, "jaeger"),
			},
			expectedSpanExporter: &jaeger.Exporter{},
		},
		{
			name: "otlp",
			args: []string{
				fmt.Sprintf("--%s", OtelTracesFlag),
				fmt.Sprintf("--%s=%s", OtelTracesExporterFlag, "otlp"),
			},
			expectedSpanExporter: &otlptrace.Exporter{},
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			cmd := &cobra.Command{
				PreRunE: func(cmd *cobra.Command, args []string) error {
					// Since we are doing multiple tests with the same otlp
					// package, we have to reset the once variables.
					otlp.OnceLoadResources = sync.Once{}
					return viper.BindPFlags(cmd.Flags())
				},
				RunE: func(cmd *cobra.Command, args []string) error {
					app := fx.New(
						fx.NopLogger,
						CLITracesModule(viper.GetViper()),
						fx.Invoke(func(lc fx.Lifecycle, spanExporter tracesdk.SpanExporter) {
							lc.Append(fx.Hook{
								OnStart: func(ctx context.Context) error {
									if !reflect.TypeOf(otel.GetTracerProvider()).
										AssignableTo(reflect.TypeOf(&tracesdk.TracerProvider{})) {
										return errors.New("otel.GetTracerProvider() should return a *tracesdk.TracerProvider instance")
									}
									if !reflect.TypeOf(spanExporter).
										AssignableTo(reflect.TypeOf(testCase.expectedSpanExporter)) {
										return fmt.Errorf("span exporter should be of type %t", testCase.expectedSpanExporter)
									}
									return nil
								},
							})
						}))
					require.NoError(t, app.Start(cmd.Context()))
					require.NoError(t, app.Err())
					return nil
				},
			}
			InitOTLPTracesFlags(cmd.Flags())

			cmd.SetArgs(testCase.args)

			require.NoError(t, cmd.Execute())
		})
	}
}
