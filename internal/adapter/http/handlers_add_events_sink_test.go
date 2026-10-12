package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleAddEventsSink_Types(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, sink string }{
		{"nats", `"nats":{"url":"nats://localhost:4222","topic":"events"}`},
		{"clickhouse", `"clickhouse":{"dsn":"clickhouse://localhost/db","table":"events"}`},
		{"kafka", `"kafka":{"brokers":["localhost:9092"],"topic":"events","tls":true,"saslMechanism":"PLAIN","saslUsername":"u","saslPassword":"secret"}`},
		{"http", `"http":{"endpoint":"https://example.com/events","secret":"secret"}`},
		{"databricks token", `"databricks":{"serverHostname":"example.com","httpPath":"/sql/warehouse","catalog":"main","schema":"default","port":443,"token":"secret"}`},
		{"databricks oauth", `"databricks":{"serverHostname":"example.com","httpPath":"/sql/warehouse","oauthM2m":{"clientId":"id","clientSecret":"secret"}}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			body := `{"name":"sink","format":"json","batchSize":8,"batchDelayMs":"20","controllerId":"owner","eventTypes":["COMMITTED_TRANSACTION"],` + tc.sink + `}`
			expected := &commonpb.SinkConfig{}
			require.NoError(t, protojson.Unmarshal([]byte(body), expected))
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				require.Equal(t, "key", req.GetUnsigned().GetIdempotencyKey())
				require.Len(t, req.GetUnsigned().GetRequests(), 1)
				config := req.GetUnsigned().GetRequests()[0].GetAddEventsSink().GetConfig()
				require.True(t, proto.Equal(expected, config), "all sink fields must reach Apply unchanged")

				return &domain.ApplyResult{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: config}}}}}}, nil
			})
			r := newRequest(t, http.MethodPost, "/_/events-sinks", strings.NewReader(body), nil)
			r.Header.Set("Idempotency-Key", "key")
			r.ContentLength = -1
			w := httptest.NewRecorder()
			newTestServer(t, backend).handleAddEventsSink(w, r)
			require.Equal(t, http.StatusCreated, w.Code)
			require.JSONEq(t, `{"data":{"name":"sink"}}`, w.Body.String())
			require.NotContains(t, w.Body.String(), "secret")
		})
	}
}

func TestHandleAddEventsSink_InvalidJSON(t *testing.T) {
	t.Parallel()
	for _, body := range []string{"", `{`, `null`, `{}`, `{"name":"sink"}`, `{"http":{"endpoint":"x"}}`,
		`{"name":"sink","http":{},"nats":{}}`, `{"name":"sink","http":{},"unknown":"secret"}`,
		`{"name":"sink","http":{}} {}`, `{"name":"sink","http":{},"batchSize":"secret"}`,
		`{"name":"sink","http":{},"eventTypes":["unknown"]}`, `{"name":"sink","name":"other","http":{}}`,
	} {
		t.Run(body, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			w := httptest.NewRecorder()
			newTestServer(t, backend).handleAddEventsSink(w, newRequest(t, http.MethodPost, "/_/events-sinks", strings.NewReader(body), nil))
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), "INVALID_REQUEST")
			require.NotContains(t, w.Body.String(), "secret")
		})
	}
}

func TestHandleAddEventsSink_Errors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"duplicate", &domain.ErrSinkAlreadyExists{Name: "sink"}, 409, "SINK_ALREADY_EXISTS"},
		{"batch size", &domain.ErrSinkBatchSizeTooLarge{Name: "sink", BatchSize: domain.MaxSinkBatchSize + 1, Max: domain.MaxSinkBatchSize}, 400, "SINK_BATCH_SIZE_TOO_LARGE"},
		{"internal", errors.New("private failure"), 500, "INTERNAL_ERROR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, tc.err)
			w := httptest.NewRecorder()
			newTestServer(t, backend).handleAddEventsSink(w, newRequest(t, http.MethodPost, "/_/events-sinks", strings.NewReader(`{"name":"sink","http":{"endpoint":"https://example.com"}}`), nil))
			require.Equal(t, tc.status, w.Code)
			require.Contains(t, w.Body.String(), tc.code)
			require.NotContains(t, w.Body.String(), "private failure")
		})
	}
}

func TestHandleAddEventsSink_BodyLimit(t *testing.T) {
	t.Parallel()
	backend := NewMockBackend(gomock.NewController(t))
	for _, body := range []string{strings.Repeat(" ", 1025), `{"name":"sink","http":{}}` + strings.Repeat(" ", 1025)} {
		w := httptest.NewRecorder()
		r := newRequest(t, http.MethodPost, "/_/events-sinks", strings.NewReader(body), nil)
		r.Body = http.MaxBytesReader(w, r.Body, 1024)
		r.ContentLength = -1
		newTestServer(t, backend).handleAddEventsSink(w, r)
		require.Equal(t, http.StatusRequestEntityTooLarge, w.Code)
		require.Contains(t, w.Body.String(), "BODY_TOO_LARGE")
	}
}
