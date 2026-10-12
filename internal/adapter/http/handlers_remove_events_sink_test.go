package http

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleRemoveEventsSink(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ name, owner, target string }{
		{"manual", "", "/v3/_/events-sinks/manual"},
		{"managed", "owner", "/v3/_/events-sinks/managed?controllerId=owner"},
		{"with/slash", "", "/v3/_/events-sinks/with%2Fslash"},
		{"literal%2Fslash", "", "/v3/_/events-sinks/literal%252Fslash"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, req *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				require.Equal(t, "key", req.GetUnsigned().GetIdempotencyKey())
				require.Len(t, req.GetUnsigned().GetRequests(), 1)
				remove := req.GetUnsigned().GetRequests()[0].GetRemoveEventsSink()
				require.Equal(t, tc.name, remove.GetName())
				require.Equal(t, tc.owner, remove.GetControllerId())

				return &domain.ApplyResult{}, nil
			})
			w := httptest.NewRecorder()
			r := httptest.NewRequest(http.MethodDelete, tc.target, nil)
			r.Header.Set("Idempotency-Key", "key")
			NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{}).ServeHTTP(w, r)
			require.Equal(t, http.StatusNoContent, w.Code)
			require.Empty(t, w.Body.String())
			require.Empty(t, w.Header().Get("Content-Type"))
		})
	}
}

func TestHandleRemoveEventsSink_Errors(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name   string
		err    error
		status int
		code   string
	}{
		{"not found", &domain.ErrSinkNotFound{Name: "sink"}, 404, "SINK_NOT_FOUND"},
		{"controller mismatch", &domain.ErrSinkControllerMismatch{Name: "sink", ControllerID: "wrong"}, 409, "SINK_CONTROLLER_MISMATCH"},
		{"internal", errors.New("private failure"), 500, "INTERNAL_ERROR"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			backend.EXPECT().Apply(gomock.Any(), gomock.Any()).Return(nil, tc.err)
			w := httptest.NewRecorder()
			newTestServer(t, backend).handleRemoveEventsSink(w, newRequest(t, http.MethodDelete, "/_/events-sinks/sink", nil, map[string]string{"sinkName": "sink"}))
			require.Equal(t, tc.status, w.Code)
			require.Contains(t, w.Body.String(), tc.code)
			require.NotContains(t, w.Body.String(), "private failure")
		})
	}
}

func TestHandleRemoveEventsSink_MissingName(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	backend := NewMockBackend(gomock.NewController(t))
	newTestServer(t, backend).handleRemoveEventsSink(w, newRequest(t, http.MethodDelete, "/_/events-sinks/", nil, nil))
	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleRemoveEventsSink_InvalidOwnershipQuery(t *testing.T) {
	t.Parallel()
	for _, query := range []string{"controllerId=%ZZ", "controllerId=owner;extra=x", "controllerId=owner&controllerId=other", "controllerId=&controllerId=owner"} {
		t.Run(query, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			w := httptest.NewRecorder()
			r := newRequest(t, http.MethodDelete, "/_/events-sinks/sink?"+query, nil, map[string]string{"sinkName": "sink"})
			newTestServer(t, backend).handleRemoveEventsSink(w, r)
			require.Equal(t, http.StatusBadRequest, w.Code)
			require.Contains(t, w.Body.String(), "INVALID_REQUEST")
		})
	}
}
