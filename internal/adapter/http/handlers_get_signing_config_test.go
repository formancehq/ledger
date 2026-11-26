package http

import (
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestHandleGetSigningConfig(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		requireSignatures bool
		body              string
	}{
		{false, `{"data":{"requireSignatures":false}}`},
		{true, `{"data":{"requireSignatures":true}}`},
	} {
		backend := NewMockBackend(gomock.NewController(t))
		backend.EXPECT().GetSigningConfig(gomock.Any()).Return(tc.requireSignatures, nil)
		srv := newTestServer(t, backend)

		w := httptest.NewRecorder()
		srv.handleGetSigningConfig(w, newRequest(t, http.MethodGet, "/_/signing-config", nil, nil))

		require.Equal(t, http.StatusOK, w.Code)
		require.JSONEq(t, tc.body, w.Body.String())
	}
}

func TestHandleGetSigningConfig_BackendError(t *testing.T) {
	t.Parallel()

	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().GetSigningConfig(gomock.Any()).Return(false, errors.New("boom"))
	srv := newTestServer(t, backend)

	w := httptest.NewRecorder()
	srv.handleGetSigningConfig(w, newRequest(t, http.MethodGet, "/_/signing-config", nil, nil))

	require.Equal(t, http.StatusInternalServerError, w.Code)
}
