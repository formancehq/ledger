package http

import (
	"context"
	"encoding/json"
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
		func(_ context.Context) ([]*commonpb.SinkConfig, []*commonpb.SinkStatus, error) {
			return []*commonpb.SinkConfig{{Name: "kafka"}},
				[]*commonpb.SinkStatus{{SinkName: "kafka", Cursor: 42}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/events-sinks", nil, nil)

	srv.handleGetEventsSinks(w, r)

	require.Equal(t, http.StatusOK, w.Code)

	// The HTTP endpoint must expose the per-sink status data at parity with
	// gRPC, not just the configs (EN-1472).
	var body struct {
		Data struct {
			Sinks        []map[string]any `json:"sinks"`
			SinkStatuses []map[string]any `json:"sinkStatuses"`
		} `json:"data"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Len(t, body.Data.Sinks, 1)
	require.Len(t, body.Data.SinkStatuses, 1)
	require.Equal(t, "kafka", body.Data.SinkStatuses[0]["sinkName"])
}

func TestHandleGetEventsSinks_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetEventsSinks(gomock.Any()).DoAndReturn(
		func(_ context.Context) ([]*commonpb.SinkConfig, []*commonpb.SinkStatus, error) {
			return nil, nil, errors.New("boom")
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/_/events-sinks", nil, nil)

	srv.handleGetEventsSinks(w, r)

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
