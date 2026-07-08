package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestHandleGetEventsSinks_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetEventsSinks(gomock.Any()).DoAndReturn(
		func(_ context.Context) ([]*commonpb.SinkConfig, error) {
			return []*commonpb.SinkConfig{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/events-sinks", nil, nil)

	srv.handleGetEventsSinks(w, r)

	require.Equal(t, http.StatusOK, w.Code)
}

func TestHandleGetEventsSinks_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetEventsSinks(gomock.Any()).DoAndReturn(
		func(_ context.Context) ([]*commonpb.SinkConfig, error) {
			return nil, errors.New("boom")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/events-sinks", nil, nil)

	srv.handleGetEventsSinks(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
