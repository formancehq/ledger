package http

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/proto/restorepb"
)

func TestIsLoopbackHost(t *testing.T) {
	t.Parallel()
	for host, want := range map[string]bool{
		"127.0.0.1": true,
		"127.0.0.2": true,
		"::1":       true,
		"localhost": true,
		"0.0.0.0":   false,
		"::":        false,
		"10.0.0.5":  false,
		"ledger-0":  false,
		"":          false,
	} {
		require.Equal(t, want, IsLoopbackHost(host), host)
	}
}

// TestRestoreRequestGuard checks that every request a web page can make is
// refused before it reaches the restore service, while CLI-shaped requests
// pass.
func TestRestoreRequestGuard(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		method  string
		target  string
		host    string
		headers map[string]string
		status  int
	}{
		{"loopback ipv4", http.MethodPost, "/v3/_/restore/finalize", "127.0.0.1:9000", map[string]string{"Content-Type": "application/json"}, http.StatusOK},
		{"loopback ipv6", http.MethodPost, "/v3/_/restore/finalize", "[::1]:9000", map[string]string{"Content-Type": "application/json; charset=utf-8"}, http.StatusOK},
		{"localhost without port", http.MethodGet, "/v3/_/restore/preview", "localhost", nil, http.StatusOK},
		{"user navigation", http.MethodGet, "/v3/_/restore/preview", "localhost:9000", map[string]string{"Sec-Fetch-Site": "none"}, http.StatusOK},
		{"rebound host", http.MethodPost, "/v3/_/restore/finalize", "attacker.example:9000", map[string]string{"Content-Type": "application/json"}, http.StatusForbidden},
		{"cross-site fetch", http.MethodPost, "/v3/_/restore/finalize", "127.0.0.1:9000", map[string]string{"Content-Type": "application/json", "Sec-Fetch-Site": "cross-site"}, http.StatusForbidden},
		{"same-site fetch", http.MethodGet, "/v3/_/restore/preview", "127.0.0.1:9000", map[string]string{"Sec-Fetch-Site": "same-site"}, http.StatusForbidden},
		{"origin", http.MethodDelete, "/v3/_/restore/downloads/job", "127.0.0.1:9000", map[string]string{"Origin": "https://attacker.example"}, http.StatusForbidden},
		{"null origin", http.MethodPost, "/v3/_/restore/finalize", "127.0.0.1:9000", map[string]string{"Content-Type": "application/json", "Origin": "null"}, http.StatusForbidden},
		{"simple post", http.MethodPost, "/v3/_/restore/finalize", "127.0.0.1:9000", map[string]string{"Content-Type": "text/plain"}, http.StatusUnsupportedMediaType},
		{"post without type", http.MethodPost, "/v3/_/restore/validate", "127.0.0.1:9000", nil, http.StatusUnsupportedMediaType},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockRestoreBackend(gomock.NewController(t))
			if tc.status == http.StatusOK {
				backend.EXPECT().FinalizeRestore(gomock.Any(), gomock.Any()).Return(&restorepb.FinalizeRestoreResponse{}, nil).AnyTimes()
				backend.EXPECT().PreviewRestore(gomock.Any(), gomock.Any()).Return(&restorepb.PreviewRestoreResponse{}, nil).AnyTimes()
			}
			r := httptest.NewRequest(tc.method, tc.target, nil)
			r.Host = tc.host
			for k, v := range tc.headers {
				r.Header.Set(k, v)
			}
			w := httptest.NewRecorder()
			NewRestoreHandler(logging.Testing(), backend).ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
		})
	}
}

func TestRestoreRequestGuard_HealthUnguarded(t *testing.T) {
	t.Parallel()
	r := httptest.NewRequest(http.MethodGet, "/health", nil)
	r.Header.Set("Origin", "https://attacker.example")
	w := httptest.NewRecorder()
	NewRestoreHandler(logging.Testing(), NewMockRestoreBackend(gomock.NewController(t))).ServeHTTP(w, r)
	require.Equal(t, http.StatusOK, w.Code)
}
