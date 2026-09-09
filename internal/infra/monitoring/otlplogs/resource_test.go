package otlplogs

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/log/global"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	collector "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc"

	"github.com/formancehq/go-libs/v5/pkg/observe"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
)

type resourceLogReceiver struct {
	collector.UnimplementedLogsServiceServer

	received chan *collector.ExportLogsServiceRequest
}

func (r *resourceLogReceiver) Export(ctx context.Context, request *collector.ExportLogsServiceRequest) (*collector.ExportLogsServiceResponse, error) {
	select {
	case r.received <- request:
		return &collector.ExportLogsServiceResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// Logger changes the global log provider, so this collector test runs serially.
func TestLoggerExportsSharedTraceResource(t *testing.T) {
	previousProvider := global.GetLoggerProvider()
	t.Cleanup(func() { global.SetLoggerProvider(previousProvider) })

	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	require.NoError(t, err)
	server := grpc.NewServer()
	receiver := &resourceLogReceiver{received: make(chan *collector.ExportLogsServiceRequest, 1)}
	collector.RegisterLogsServiceServer(server, receiver)
	served := make(chan error, 1)
	go func() { served <- server.Serve(listener) }()
	t.Cleanup(func() {
		server.Stop()
		require.NoError(t, <-served)
	})

	sharedResource, err := observe.BuildResource("ledger-node-42", []string{
		"service.name=ledger-custom", "service.version=overridden-build", "deployment.environment=regression",
	}, "3.0.0-build")
	require.NoError(t, err)
	spanExporter := tracetest.NewInMemoryExporter()
	traceProvider := sdktrace.NewTracerProvider(sdktrace.WithResource(sharedResource), sdktrace.WithSyncer(spanExporter))
	t.Cleanup(func() { require.NoError(t, traceProvider.Shutdown(context.Background())) })
	_, span := traceProvider.Tracer("resource-regression").Start(t.Context(), "request")
	span.End()
	spans := spanExporter.GetSpans()
	require.Len(t, spans, 1)

	logger, err := Logger(ModuleConfig{
		Exporter:   OTLPExporter,
		OTLPConfig: &OTLPConfig{Mode: "grpc", Endpoint: listener.Addr().String(), Insecure: true},
		Resource:   sharedResource, Output: io.Discard, Level: logging.InfoLevel,
	})
	require.NoError(t, err)
	provider, ok := global.GetLoggerProvider().(*sdklog.LoggerProvider)
	require.True(t, ok)
	t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })

	logger.Info("shared-resource-regression")
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	require.NoError(t, provider.ForceFlush(ctx))
	select {
	case request := <-receiver.received:
		require.Len(t, request.GetResourceLogs(), 1)
		exported := request.GetResourceLogs()[0]
		got := make(map[string]string)
		for _, attribute := range exported.GetResource().GetAttributes() {
			got[attribute.GetKey()] = attribute.GetValue().GetStringValue()
		}
		want := make(map[string]string)
		for _, attribute := range spans[0].Resource.Attributes() {
			want[string(attribute.Key)] = attribute.Value.AsString()
		}
		require.Equal(t, want, got, "logs and actual exported spans must share all resource attributes")
		require.Equal(t, spans[0].Resource.SchemaURL(), exported.GetSchemaUrl())
		require.Len(t, exported.GetScopeLogs(), 1)
		require.Len(t, exported.GetScopeLogs()[0].GetLogRecords(), 1)
		require.Equal(t, "shared-resource-regression", exported.GetScopeLogs()[0].GetLogRecords()[0].GetBody().GetStringValue())
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
}
