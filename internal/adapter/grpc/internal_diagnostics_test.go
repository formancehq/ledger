package grpc

import (
	"bytes"
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// Exercise the real reads with malformed persisted protobuf data, rather than
// handing a fabricated status directly to the conversion interceptor.
func TestInternalReadFailuresAreSanitized(t *testing.T) {
	t.Parallel()

	t.Run("staged-config", func(t *testing.T) {
		t.Parallel()
		var logs bytes.Buffer
		logger := logging.NewDefaultLogger(&logs, false, false, false)
		recorder := tracetest.NewSpanRecorder()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
		t.Cleanup(func() { require.NoError(t, provider.Shutdown(context.Background())) })
		ctx, span := provider.Tracer("test").Start(context.Background(), "staged-config")
		ctx = logging.ContextWithLogger(ctx, logger)
		server := NewRestoreServiceServer(t.TempDir(), "test-cluster", 1, logger)
		store, err := dal.OpenDirect(server.stagingDir(), logger)
		require.NoError(t, err)
		server.stagingStore = store
		server.downloaded = true
		t.Cleanup(server.Shutdown)
		batch := store.OpenWriteSession()
		require.NoError(t, batch.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobPersistedConfig}, []byte{0xff}))
		require.NoError(t, batch.Commit())
		stream := NewMockServerStreamingServer[restorepb.ValidateRestoreEvent](gomock.NewController(t))
		stream.EXPECT().Context().Return(ctx).AnyTimes()
		err = server.ValidateRestore(&restorepb.ValidateRestoreRequest{}, stream)
		diagnostic := "reading staged backup config"
		err = convertToGRPCErrorWithContext(ctx, err, logger)
		span.End()
		st := status.Convert(err)
		require.Equal(t, codes.Internal, st.Code())
		require.Regexp(t, `^internal server error \(correlation ID: [0-9a-f]+\)$`, st.Message())
		id := strings.TrimSuffix(strings.TrimPrefix(st.Message(), "internal server error (correlation ID: "), ")")
		require.Contains(t, logs.String(), id)
		require.NotContains(t, st.Message(), diagnostic)
		require.Empty(t, st.Details())
		require.Contains(t, logs.String(), diagnostic)
		require.Contains(t, logs.String(), "correlation_id")
		require.Contains(t, logs.String(), span.SpanContext().TraceID().String())
		require.Equal(t, id, grpcSpanAttribute(recorder.Ended()[0], "correlation_id"))
		require.NotEmpty(t, recorder.Ended()[0].Events())
	})
}
