package http

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestHandleSaveAccountMetadata_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _ ...*servicepb.Envelope) ([]*commonpb.Log, error) {
			return []*commonpb.Log{{}}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"role":"admin","active":"true"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/accounts/users:001/metadata", body, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
	})

	srv.handleSaveAccountMetadata(w, r)

	require.Equal(t, http.StatusNoContent, w.Code)
}

func TestHandleSaveAccountMetadata_InvalidBody(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`not json`)
	r := newRequest(t, http.MethodPost, "/ledger1/accounts/users:001/metadata", body, map[string]string{
		"ledgerName": "ledger1",
		"address":    "users:001",
	})

	srv.handleSaveAccountMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}

func TestHandleSaveAccountMetadata_MissingAddress(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	body := strings.NewReader(`{"key":"val"}`)
	r := newRequest(t, http.MethodPost, "/ledger1/accounts//metadata", body, map[string]string{
		"ledgerName": "ledger1",
		"address":    "",
	})

	srv.handleSaveAccountMetadata(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
