package http

import (
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	ggrpc "google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

func progressEvent(checked, total uint64) *restorepb.ValidateRestoreEvent {
	return &restorepb.ValidateRestoreEvent{Type: &restorepb.ValidateRestoreEvent_Progress{
		Progress: &restorepb.ValidateRestoreProgress{LogsChecked: checked, TotalLogs: total},
	}}
}

func findingEvent(message string) *restorepb.ValidateRestoreEvent {
	return &restorepb.ValidateRestoreEvent{Type: &restorepb.ValidateRestoreEvent_Error{
		Error: &restorepb.ValidateRestoreError{Message: message},
	}}
}

func validateWith(t *testing.T, run func(stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error) RestoreBackend {
	t.Helper()
	backend := NewMockRestoreBackend(gomock.NewController(t))
	backend.EXPECT().ValidateRestore(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ *restorepb.ValidateRestoreRequest, stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
			return run(stream)
		})

	return backend
}

func TestHandleValidateRestore_Passed(t *testing.T) {
	t.Parallel()
	backend := validateWith(t, func(stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
		require.NoError(t, stream.Send(progressEvent(1, 2)))
		require.NoError(t, stream.Send(progressEvent(2, 2)))

		return nil
	})
	w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/validate", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, "application/x-ndjson", w.Header().Get("Content-Type"))
	require.Equal(t, []map[string]any{
		{"progress": map[string]any{"logsChecked": float64(1), "totalLogs": float64(2)}},
		{"progress": map[string]any{"logsChecked": float64(2), "totalLogs": float64(2)}},
		{"result": map[string]any{"passed": true}},
	}, ndjsonLines(t, w))
}

func TestHandleValidateRestore_NoEvents(t *testing.T) {
	t.Parallel()
	backend := validateWith(t, func(ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error { return nil })
	w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/validate", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, []map[string]any{{"result": map[string]any{"passed": true}}}, ndjsonLines(t, w))
}

func TestHandleValidateRestore_Findings(t *testing.T) {
	t.Parallel()
	backend := validateWith(t, func(stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
		require.NoError(t, stream.Send(findingEvent("audit chain broken at sequence 7")))
		require.NoError(t, stream.Send(progressEvent(9, 9)))

		return nil
	})
	w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/validate", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Equal(t, []map[string]any{
		{"error": map[string]any{"message": "audit chain broken at sequence 7"}},
		{"progress": map[string]any{"logsChecked": float64(9), "totalLogs": float64(9)}},
		{"result": map[string]any{"passed": false}},
	}, ndjsonLines(t, w))
}

func TestHandleValidateRestore_FailsBeforeStream(t *testing.T) {
	t.Parallel()
	backend := validateWith(t, func(ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
		return status.Error(codes.FailedPrecondition, "restore validation is already running")
	})
	w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/validate", "")
	require.Equal(t, http.StatusConflict, w.Code, w.Body.String())
	resp := decodeErrorResponse(t, w)
	require.Equal(t, "FAILED_PRECONDITION", resp.ErrorCode)
	require.Equal(t, "restore validation is already running", resp.ErrorMessage)
}

func TestHandleValidateRestore_FailsMidStream(t *testing.T) {
	t.Parallel()
	backend := validateWith(t, func(stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
		require.NoError(t, stream.Send(progressEvent(1, 9)))

		return errors.New("reading /data/staging/000012.sst: input/output error")
	})
	w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/validate", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	lines := ndjsonLines(t, w)
	require.Len(t, lines, 2)
	result, ok := lines[1]["result"].(map[string]any)
	require.True(t, ok, lines[1])
	require.Equal(t, false, result["passed"])
	require.Equal(t, "INTERNAL_ERROR", result["errorCode"])
	require.NotContains(t, result["errorMessage"], "/data/staging")
}

func TestHandleValidateRestore_StreamContext(t *testing.T) {
	t.Parallel()
	backend := validateWith(t, func(stream ggrpc.ServerStreamingServer[restorepb.ValidateRestoreEvent]) error {
		require.NotNil(t, stream.Context())
		require.NoError(t, stream.SendMsg(progressEvent(1, 1)))
		require.Error(t, stream.SendMsg(&restorepb.PreviewRestoreRequest{}))

		return nil
	})
	w := serveRestore(t, backend, http.MethodPost, "/v3/_/restore/validate", "")
	require.Equal(t, http.StatusOK, w.Code, w.Body.String())
	require.Len(t, ndjsonLines(t, w), 2)
}
