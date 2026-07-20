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

func TestHandleListNumscriptVersions_Success(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().ListNumscriptVersions(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(
		func(_ context.Context, _, _ string) (string, []*commonpb.NumscriptVersionEntry, error) {
			return "2.0.0", []*commonpb.NumscriptVersionEntry{
				{Version: "2.0.0"},
				{Version: "1.0.0"},
			}, nil
		}).AnyTimes()
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/numscripts/my-script/versions", nil,
		map[string]string{"ledgerName": "ledger1", "name": "my-script"})

	srv.handleListNumscriptVersions(w, r)

	require.Equal(t, http.StatusOK, w.Code)
	require.Contains(t, w.Body.String(), `"latestVersion":"2.0.0"`)
	require.Contains(t, w.Body.String(), `"version":"1.0.0"`)
}

func TestHandleListNumscriptVersions_MissingName(t *testing.T) {
	t.Parallel()

	srv := newTestServer(t, NewMockBackend(gomock.NewController(t)))

	w := httptest.NewRecorder()
	r := newRequest(t, http.MethodGet, "/ledger1/numscripts//versions", nil,
		map[string]string{"ledgerName": "ledger1", "name": ""})

	srv.handleListNumscriptVersions(w, r)

	require.Equal(t, http.StatusBadRequest, w.Code)
}
