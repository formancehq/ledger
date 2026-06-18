package cmdutil

import (
	"context"
	"fmt"
	"os"
	"os/user"
	"strings"
	"time"

	"github.com/pterm/pterm"
	"github.com/spf13/cobra"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
	"go.opentelemetry.io/otel/trace"
)

// tracerName identifies the instrumentation scope for spans created directly by
// ledgerctl (the per-invocation root span). RPC spans are created by the
// otelgrpc client stats handler under its own scope.
const tracerName = "github.com/formancehq/ledger/v3/cmd/ledgerctl"

// defaultServiceName is the service.name reported on spans when the standard
// OTEL_SERVICE_NAME env var is not set.
const defaultServiceName = "ledgerctl"

// otelShutdownTimeout bounds the time spent flushing buffered spans on exit so a
// dead or slow collector can never hang the CLI after the command has finished.
const otelShutdownTimeout = 5 * time.Second

// SetupTracing installs a global OpenTelemetry tracer provider and W3C trace
// context propagator, configured entirely from the standard OTEL_* environment
// variables documented by the OpenTelemetry SDK
// (https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/).
//
// Propagation is always enabled so the server-side spans share this CLI
// invocation's trace ID; spans are only exported to a backend when an OTLP
// endpoint is configured (OTEL_EXPORTER_OTLP_ENDPOINT /
// OTEL_EXPORTER_OTLP_TRACES_ENDPOINT) or OTEL_TRACES_EXPORTER=otlp is set.
// Without an endpoint the CLI never attempts a connection, so default usage is
// unaffected. OTEL_SDK_DISABLED=true disables tracing entirely.
//
// It always returns a non-nil shutdown func; the caller must invoke it before
// the process exits to flush buffered spans. Exporter misconfiguration is
// reported as a warning and degrades to propagation-only — it never blocks the
// command from running.
func SetupTracing(ctx context.Context, serviceVersion string) func(context.Context) {
	noop := func(context.Context) {}

	// Shell completion (cobra's hidden __complete* commands) runs on every TAB
	// press and may itself issue RPCs (e.g. --ledger name completion). Keep it
	// fast and side-effect free: never initialise an exporter or emit spans for
	// a completion request.
	if isCompletionRequest() {
		return noop
	}

	if strings.EqualFold(strings.TrimSpace(os.Getenv("OTEL_SDK_DISABLED")), "true") {
		return noop
	}

	// Propagate trace context to the server regardless of whether we export, so
	// the server's spans join this invocation's trace.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	res, err := buildResource(ctx, serviceVersion)
	if err != nil {
		pterm.Warning.Printfln("OpenTelemetry: failed to build resource, using default: %v", err)

		res = resource.Default()
	}

	opts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.ParentBased(sdktrace.AlwaysSample())),
	}

	exporter, err := newSpanExporter(ctx)
	if err != nil {
		// Don't fail the command — fall back to propagation-only tracing.
		pterm.Warning.Printfln("OpenTelemetry: trace export disabled: %v", err)
	} else if exporter != nil {
		opts = append(opts, sdktrace.WithBatcher(exporter))
	}

	tp := sdktrace.NewTracerProvider(opts...)
	otel.SetTracerProvider(tp)

	return func(parent context.Context) {
		shutdownCtx, cancel := context.WithTimeout(parent, otelShutdownTimeout)
		defer cancel()

		_ = tp.ForceFlush(shutdownCtx)
		_ = tp.Shutdown(shutdownCtx)
	}
}

// buildResource describes this CLI process. service.name defaults to
// defaultServiceName and service.version to the build version, but both — along
// with any extra attributes — are overridden by OTEL_SERVICE_NAME /
// OTEL_RESOURCE_ATTRIBUTES because WithFromEnv is merged last.
func buildResource(ctx context.Context, serviceVersion string) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithAttributes(
			semconv.ServiceName(defaultServiceName),
			semconv.ServiceVersion(serviceVersion),
		),
		resource.WithFromEnv(),
	)
}

// newSpanExporter builds an OTLP span exporter from the standard OTEL_* env
// vars, or returns (nil, nil) when no export is configured. The exporters read
// their endpoint, headers, TLS and timeout settings directly from the
// environment; only the protocol selection (grpc vs. http) is resolved here.
func newSpanExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	exporter := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_TRACES_EXPORTER")))
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}

	switch exporter {
	case "none":
		return nil, nil
	case "", "otlp":
		// Default ("") only enables export when an endpoint is configured, so we
		// never blindly dial localhost:4317 on a routine CLI invocation.
		if exporter == "" && endpoint == "" {
			return nil, nil
		}
	default:
		return nil, fmt.Errorf("unsupported OTEL_TRACES_EXPORTER %q (only \"otlp\" is supported)", exporter)
	}

	protocol := strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_TRACES_PROTOCOL")))
	if protocol == "" {
		protocol = strings.ToLower(strings.TrimSpace(os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL")))
	}
	if protocol == "" {
		protocol = "grpc"
	}

	switch protocol {
	case "grpc":
		return otlptracegrpc.New(ctx)
	case "http/protobuf", "http/json", "http":
		return otlptracehttp.New(ctx)
	default:
		return nil, fmt.Errorf("unsupported OTLP protocol %q (use \"grpc\" or \"http/protobuf\")", protocol)
	}
}

// isCompletionRequest reports whether this process was invoked by cobra's shell
// completion machinery (the hidden __complete / __completeNoDesc commands the
// shell calls on TAB), rather than by a user running a real command.
func isCompletionRequest() bool {
	if len(os.Args) < 2 {
		return false
	}

	switch os.Args[1] {
	case cobra.ShellCompRequestCmd, cobra.ShellCompNoDescRequestCmd:
		return true
	default:
		return false
	}
}

// StartRootSpan opens the per-invocation root span. RPC spans created by the
// otelgrpc client handler nest under it because the returned context is threaded
// through cobra via ExecuteContext. The span is named generically here; once the
// target subcommand is known, NameCommandSpan refines it.
func StartRootSpan(ctx context.Context) (context.Context, trace.Span) {
	return otel.Tracer(tracerName).Start(ctx, "ledgerctl",
		trace.WithAttributes(invocationAttributes()...),
	)
}

// invocationAttributes captures who ran the CLI and from where, so a trace can
// be attributed to a host and an operator. Each attribute is best-effort:
// lookups that fail (e.g. user.Current in a static binary without cgo) are
// simply omitted rather than failing the command.
func invocationAttributes() []attribute.KeyValue {
	attrs := make([]attribute.KeyValue, 0, 2)

	if host, err := os.Hostname(); err == nil && host != "" {
		attrs = append(attrs, semconv.HostName(host))
	}

	if u, err := user.Current(); err == nil && u.Username != "" {
		attrs = append(attrs, attribute.String("host.user", u.Username))
	}

	return attrs
}

// NameCommandSpan renames the active root span to the resolved command path
// (e.g. "ledgerctl transactions get") and records the invoked command as an
// attribute. It is a no-op when no recording span is active.
func NameCommandSpan(cmd *cobra.Command) {
	span := trace.SpanFromContext(cmd.Context())
	if !span.IsRecording() {
		return
	}

	span.SetName(cmd.CommandPath())
	span.SetAttributes(attribute.String("ledgerctl.command", cmd.CommandPath()))
}

// RecordSpanError marks the active span (from cmd's context) as failed. Used on
// the top-level error path so a failed invocation is visible in the trace.
func RecordSpanError(span trace.Span, err error) {
	if err == nil || !span.IsRecording() {
		return
	}

	span.RecordError(err)
	span.SetStatus(codes.Error, err.Error())
}
