package server

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/fx"

	"github.com/formancehq/go-libs/v5/pkg/fx/observefx"
	otlptraces "github.com/formancehq/go-libs/v5/pkg/observe/traces"
	"github.com/formancehq/go-libs/v5/pkg/service"
)

func TestServerTraceSamplerSDK(t *testing.T) {
	// Fx installs a global provider/propagator; keep these cases serial and restore
	// them before other tests run. Environment variables are process-wide too.
	originalProvider := otel.GetTracerProvider()
	originalPropagator := otel.GetTextMapPropagator()
	t.Cleanup(func() {
		otel.SetTracerProvider(originalProvider)
		otel.SetTextMapPropagator(originalPropagator)
	})
	for _, tt := range []struct {
		name          string
		flags         []string
		envPolicy     string
		envArg        string
		typedPolicy   string
		typedArg      string
		highTraceID   bool
		wantRoot      bool
		wantUnsampled bool
		wantSampled   bool
	}{
		{name: "default", wantRoot: true, wantSampled: true},
		{name: "always on", flags: []string{"--trace-sampler=always_on"}, wantRoot: true, wantUnsampled: true, wantSampled: true},
		{name: "always off", flags: []string{"--trace-sampler=always_off"}},
		{name: "parent on", flags: []string{"--trace-sampler=parentbased_always_on"}, wantRoot: true, wantSampled: true},
		{name: "parent off", flags: []string{"--trace-sampler=parentbased_always_off"}, wantSampled: true},
		{name: "ratio zero", flags: []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=0"}},
		{name: "ratio one", flags: []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=1"}, wantRoot: true, wantUnsampled: true, wantSampled: true},
		{name: "parent ratio zero", flags: []string{"--trace-sampler=parentbased_traceidratio", "--trace-sampler-arg=0"}, wantSampled: true},
		{name: "parent ratio one", flags: []string{"--trace-sampler=parentbased_traceidratio", "--trace-sampler-arg=1"}, wantRoot: true, wantSampled: true},
		{name: "ratio fractional lower ID", flags: []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=0.5"}, wantRoot: true, wantUnsampled: true, wantSampled: true},
		{name: "ratio fractional upper ID", flags: []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=0.5"}, highTraceID: true},
		{name: "parent ratio fractional lower ID", flags: []string{"--trace-sampler=parentbased_traceidratio", "--trace-sampler-arg=0.5"}, wantRoot: true, wantSampled: true},
		{name: "parent ratio fractional upper ID", flags: []string{"--trace-sampler=parentbased_traceidratio", "--trace-sampler-arg=0.5"}, highTraceID: true, wantSampled: true},
		{name: "typed environment", typedPolicy: "traceidratio", typedArg: "0"},
		{name: "CLI overrides typed environment", flags: []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=1"}, typedPolicy: "traceidratio", typedArg: "0", wantRoot: true, wantUnsampled: true, wantSampled: true},
		{name: "raw SDK policy", envPolicy: "always_off"},
		{name: "raw missing ratio retains SDK default", envPolicy: "traceidratio", wantRoot: true, wantUnsampled: true, wantSampled: true},
		{name: "raw ratio", envPolicy: "parentbased_traceidratio", envArg: "0", wantSampled: true},
		{name: "flags override raw pair", flags: []string{"--trace-sampler=always_on"}, envPolicy: "traceidratio", envArg: "0", wantRoot: true, wantUnsampled: true, wantSampled: true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_TRACES_SAMPLER", tt.envPolicy)
			t.Setenv("OTEL_TRACES_SAMPLER_ARG", tt.envArg)
			// The SDK distinguishes an absent argument from an empty one.
			if tt.envArg == "" {
				require.NoError(t, os.Unsetenv("OTEL_TRACES_SAMPLER_ARG"))
			}
			if tt.envPolicy == "" {
				require.NoError(t, os.Unsetenv("OTEL_TRACES_SAMPLER"))
			}
			t.Setenv("TRACE_SAMPLER", tt.typedPolicy)
			t.Setenv("TRACE_SAMPLER_ARG", tt.typedArg)
			cmd := NewRunCommand()
			require.NoError(t, service.BindEnvToFlagSetWithError(cmd.Flags()))
			require.NoError(t, cmd.ParseFlags(tt.flags))
			require.NoError(t, cmd.Flags().Set(otlptraces.OtelTracesExporterFlag, "stdout"))
			module, err := tracesModuleFromFlags(cmd)
			require.NoError(t, err)
			exporter := tracetest.NewInMemoryExporter()
			var tp *sdktrace.TracerProvider
			traceID := trace.TraceID{1}
			if tt.highTraceID {
				traceID[8] = 0xff
			}
			app := fx.New(fx.NopLogger, module, fx.Supply(resource.Empty()),
				fx.Replace(fx.Annotate(exporter, fx.As(new(sdktrace.SpanExporter)))),
				observefx.ProvideTracerProviderOption(func() sdktrace.TracerProviderOption { return sdktrace.WithIDGenerator(samplingTestIDs{traceID}) }),
				fx.Populate(&tp))
			require.NoError(t, app.Err())
			require.NoError(t, app.Start(t.Context()))
			t.Cleanup(func() { require.NoError(t, app.Stop(context.Background())) })
			for _, parent := range []struct {
				name string
				ctx  context.Context
				want bool
			}{
				{"root", t.Context(), tt.wantRoot},
				{"unsampled", trace.ContextWithRemoteSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: trace.SpanID{1}, Remote: true})), tt.wantUnsampled},
				{"sampled", trace.ContextWithRemoteSpanContext(t.Context(), trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: trace.SpanID{1}, TraceFlags: trace.FlagsSampled, Remote: true})), tt.wantSampled},
			} {
				_, span := tp.Tracer("sampling-test").Start(parent.ctx, parent.name)
				require.Equal(t, parent.want, span.IsRecording(), parent.name)
				span.End()
			}
			require.NoError(t, tp.ForceFlush(t.Context()))
			count := 0
			for _, want := range []bool{tt.wantRoot, tt.wantUnsampled, tt.wantSampled} {
				if want {
					count++
				}
			}
			require.Len(t, exporter.GetSpans(), count)
		})
	}
}

func TestServerTraceSamplerValidation(t *testing.T) {
	t.Parallel()
	for _, tt := range []struct {
		name    string
		flags   []string
		message string
	}{
		{"unknown", []string{"--trace-sampler=invalid"}, "unsupported"},
		{"missing ratio", []string{"--trace-sampler=traceidratio"}, "requires"},
		{"missing parent ratio", []string{"--trace-sampler=parentbased_traceidratio"}, "requires"},
		{"arg only", []string{"--trace-sampler-arg=0.5"}, "requires"},
		{"unused arg", []string{"--trace-sampler=always_on", "--trace-sampler-arg=0.5"}, "only"},
		{"negative", []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=-0.1"}, "finite"},
		{"large", []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=1.1"}, "finite"},
		{"nan", []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=NaN"}, "finite"},
		{"infinity", []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=+Inf"}, "finite"},
		{"text", []string{"--trace-sampler=traceidratio", "--trace-sampler-arg=bad"}, "finite"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cmd := NewRunCommand()
			cmd.SetArgs(tt.flags)
			// Execute the real RunE: sampler validation precedes storage/network setup.
			require.ErrorContains(t, cmd.Execute(), tt.message)
		})
	}
}

// Fixed IDs make fractional sampling deterministic at the SDK boundary.
type samplingTestIDs struct{ id trace.TraceID }

func (g samplingTestIDs) NewIDs(context.Context) (trace.TraceID, trace.SpanID) {
	return g.id, trace.SpanID{1}
}

func (g samplingTestIDs) NewSpanID(context.Context, trace.TraceID) trace.SpanID {
	return trace.SpanID{2}
}
