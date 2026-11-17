package http

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
)

// Every rejection goes through the public router. A nil service ensures an
// accidentally accepted request cannot be mistaken for a successful rejection.
func TestAtomicApplyHTTPRejectsInvalidRequests(t *testing.T) {
	t.Parallel()
	const valid = `{"requests":[{"createLedger":{"name":"one"}}]}`
	for _, tc := range []struct {
		name, body, query string
		headers           http.Header
		status            int
		code              string
	}{
		{name: "empty batch", body: `{"requests":[]}`, status: 400, code: "INVALID_REQUEST"},
		{name: "missing operation", body: `{"requests":[{}]}`, status: 400, code: "INVALID_REQUEST"},
		{name: "null operation", body: `{"requests":[null]}`, status: 400, code: "INVALID_REQUEST"},
		{name: "body key", body: `{"requests":[{"createLedger":{"name":"one"}}],"idempotencyKey":"x"}`, status: 400, code: "INVALID_REQUEST"},
		{name: "duplicate header", body: valid, headers: http.Header{"Idempotency-Key": {"one", "two"}}, status: 400, code: "INVALID_REQUEST"},
		{name: "trailing value", body: valid + `{}`, status: 400, code: "INVALID_REQUEST"},
		{name: "unknown option", body: valid, query: "?atomic=false", status: 400, code: "INVALID_REQUEST"},
		{name: "oversized body", body: valid + strings.Repeat(" ", int(defaultMaxBodySize)), status: 413, code: "BODY_TOO_LARGE"},
		{name: "too many requests", body: `{"requests":[` + strings.Repeat(`{"createLedger":{"name":"one"}},`, defaultBulkMaxSize) + `{"createLedger":{"name":"one"}}]}`, status: 413, code: "BULK_SIZE_EXCEEDED"},
		{name: "signed mismatch", body: `{"request":` + valid + `,"payload":"AQ=="}`, headers: http.Header{"Formance-Signing-Key-Id": {"key"}, "Formance-Signature": {base64.StdEncoding.EncodeToString(make([]byte, 64))}}, status: 403, code: "PERMISSION_DENIED"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			router := NewHandler(logging.Testing(), NewMockBackend(gomock.NewController(t)), internalauth.AuthConfig{}, version.Info{}, nil)
			r := httptest.NewRequest(http.MethodPost, "/v3/_/apply"+tc.query, strings.NewReader(tc.body))
			if tc.headers != nil {
				r.Header = tc.headers.Clone()
			}
			r.Header.Set("Content-Type", "application/json")
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			require.Equal(t, tc.code, decodeResponse[ErrorResponse](t, w).ErrorCode)
		})
	}
}

func TestAtomicApplyHTTPAuthorizesEveryOperation(t *testing.T) {
	t.Parallel()
	key, keySet := testKeyPair(t)
	body := `{"requests":[{"apply":{"ledger":"one","action":{"createTransaction":{"postings":[{"source":"world","destination":"bank","amount":1,"asset":"USD"}]}}}},{"setSigningConfig":{"requireSignatures":false}}]}`
	for _, tc := range []struct {
		name, token string
		status      int
	}{
		{"anonymous", "", 401},
		{"missing later scope", signToken(t, key, testClaims("ledger:WriteTransaction")), 403},
		{"missing first scope", signToken(t, key, testClaims("ledger:WriteOps")), 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			router := NewHandler(logging.Testing(), NewMockBackend(gomock.NewController(t)), internalauth.AuthConfig{Enabled: true, KeySet: keySet, Issuers: []string{testAuthIssuer}, Service: "ledger", ScopeMapping: internalauth.DefaultMapping("ledger")}, version.Info{}, nil)
			r := httptest.NewRequest(http.MethodPost, "/v3/_/apply", strings.NewReader(body))
			r.Header.Set("Content-Type", "application/json")
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			require.Contains(t, w.Body.String(), "requires scope")
		})
	}
}
