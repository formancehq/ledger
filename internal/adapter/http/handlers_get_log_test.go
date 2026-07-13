package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetLog_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLog(gomock.Any(), uint64(7)).DoAndReturn(
		func(_ context.Context, _ uint64) (*commonpb.Log, error) {
			return &commonpb.Log{}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/7", nil, map[string]string{
		"sequence": "7",
	})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleGetLog_InvalidSequence(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/abc", nil, map[string]string{
		"sequence": "abc",
	})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleGetLog_NotFound(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetLog(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ uint64) (*commonpb.Log, error) {
			return nil, commonpb.NewNotFoundError("log %d not found", 9999)
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/logs/9999", nil, map[string]string{
		"sequence": "9999",
	})

	srv.handleGetLog(w, r)

	require.Equal(t, http.StatusNotFound, w.Code)
}
