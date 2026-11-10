package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/adapter/grpcerr"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

func TestSignedHTTPRoutes_ForwardOriginalEnvelope(t *testing.T) {
	t.Parallel()
	payload := &servicepb.CreateTransactionPayload{}
	body := []byte(`{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD"}]}`)
	require.NoError(t, json.Unmarshal(body, payload))
	request := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "ledger", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: payload}}}}}
	signed, err := signing.Sign(&servicepb.ApplyBatch{IdempotencyKey: "signed-key", Requests: []*servicepb.Request{request}}, "key", ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)))
	require.NoError(t, err)
	wrapper, err := json.Marshal(struct {
		Request json.RawMessage `json:"request"`
		Payload []byte          `json:"payload"`
	}{body, signed.GetPayload()})
	require.NoError(t, err)
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, actual *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
		require.True(t, proto.Equal(signed, actual.GetSigned()), "HTTP must forward original client envelope unchanged")

		return &domain.ApplyResult{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1}}}}}}}}}}}, nil
	})
	router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
	r := httptest.NewRequest(http.MethodPost, "/v3/ledger/transactions", bytes.NewReader(wrapper))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("Idempotency-Key", "signed-key")
	r.Header.Set("Formance-Signing-Key-Id", signed.GetKeyId())
	r.Header.Set("Formance-Signature", base64.StdEncoding.EncodeToString(signed.GetSignature()))
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
}

// These cases exercise the public router and fail before the mocked controller
// port. A successful Apply is forbidden: parsing or route binding must reject.
func TestSignedHTTPRoutes_RejectMalformedEnvelope(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name, method, path, body string
		status                   int
	}{
		{"missing request", "POST", "/v3/ledger/transactions", `{"payload":"AQ=="}`, 400},
		{"missing payload", "POST", "/v3/ledger/transactions", `{"request":{}}`, 400},
		{"null payload", "POST", "/v3/ledger/transactions", `{"request":{},"payload":null}`, 400},
		{"empty payload", "POST", "/v3/ledger/transactions", `{"request":{},"payload":""}`, 400},
		{"non-string payload", "POST", "/v3/ledger/transactions", `{"request":{},"payload":1}`, 400},
		{"outer array", "POST", "/v3/ledger/transactions", `[]`, 400},
		{"unknown outer field", "POST", "/v3/ledger/transactions", `{"request":{},"payload":"AQ==","extra":1}`, 400},
		{"duplicate request", "POST", "/v3/ledger/transactions", `{"request":{},"request":{},"payload":"AQ=="}`, 400},
		{"duplicate payload", "POST", "/v3/ledger/transactions", `{"request":{},"payload":"AQ==","payload":"AQ=="}`, 400},
		{"body signature", "POST", "/v3/ledger/transactions", `{"request":{},"payload":"AQ==","signature":"AQ=="}`, 400},
		{"body key ID", "POST", "/v3/ledger/transactions", `{"request":{},"payload":"AQ==","keyId":"key"}`, 400},
		{"body envelope", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":{"payload":"AQ=="}}`, 400},
		{"invalid base64", "POST", "/v3/ledger/transactions", `{"request":{},"payload":"!"}`, 400},
		{"trailing JSON", "POST", "/v3/ledger/transactions", `{"request":{},"payload":"AQ=="} {}`, 400},
		{"truncated JSON", "POST", "/v3/ledger/transactions", `{"request":{},"payload":"AQ=="`, 400},
		{"delete body", "DELETE", "/v3/ledger/transactions/1/metadata/key", `{"request":{},"payload":"AQ=="}`, 400},
		{"sequential bulk", "POST", "/v3/ledger/bulk", `{"request":[{}],"payload":"AQ=="}`, 400},
		{"empty atomic bulk", "POST", "/v3/ledger/bulk?atomic=true", `{"request":[],"payload":"AQ=="}`, 400},
		{"unsupported ledger create", "POST", "/v3/ledger", `{"request":{},"payload":"AQ=="}`, 415},
		{"unsupported ledger delete", "DELETE", "/v3/ledger", `{"request":null,"payload":"AQ=="}`, 415},
		{"unsupported account metadata", "DELETE", "/v3/ledger/accounts/bank/metadata/key", `{"request":null,"payload":"AQ=="}`, 415},
		{"oversized suffix", "POST", "/v3/ledger/transactions", `{"request":{},"payload":"AQ=="}` + strings.Repeat(" ", int(defaultMaxBodySize)), 413},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/json; charset=utf-8")
			r.Header.Set("Formance-Signing-Key-Id", "key")
			r.Header.Set("Formance-Signature", base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize)))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
		})
	}
}

func TestSignedHTTPRoutes_KeepJWTScopes(t *testing.T) {
	t.Parallel()
	key, keySet := testKeyPair(t)
	for _, tc := range []struct {
		name, token string
		status      int
	}{
		{"anonymous", "", 401},
		{"read only", signToken(t, key, testClaims("ledger:ReadTransaction")), 403},
		{"transaction writer cannot write metadata", signToken(t, key, testClaims("ledger:WriteTransaction")), 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{Enabled: true, KeySet: keySet, Issuers: []string{testAuthIssuer}, Service: "ledger", ScopeMapping: internalauth.DefaultMapping("ledger")}, version.Info{})
			path := "/v3/ledger/transactions"
			if tc.name == "transaction writer cannot write metadata" {
				path += "/1/metadata"
			}
			r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"request":{},"payload":"AQ=="}`))
			r.Header.Set("Content-Type", "application/json")
			if tc.token != "" {
				r.Header.Set("Authorization", "Bearer "+tc.token)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
		})
	}
}

func TestSigningRejectionsHTTP_ForwardingParity(t *testing.T) {
	t.Parallel()
	for _, err := range []error{signing.ErrMissingSignature, signing.ErrInvalidSignature, signing.ErrUnknownKeyID} {
		t.Run(err.Error(), func(t *testing.T) {
			t.Parallel()
			local := classifyError(err)
			remote := classifyError(grpcerr.FromStatusError(grpcerr.SigningStatus(err).Err()))
			require.Equal(t, local, remote)
			require.False(t, remote.sanitized)
		})
	}
	for _, code := range []codes.Code{codes.Unauthenticated, codes.PermissionDenied} {
		c := classifyError(grpcerr.FromStatusError(status.Error(code, "private cluster-secret failure")))
		require.Equal(t, http.StatusInternalServerError, c.status)
		require.True(t, c.sanitized)
	}
}

func TestSignedHTTPRoutes_RejectMalformedSigningHeaders(t *testing.T) {
	t.Parallel()
	validSignature := base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize))
	for _, tc := range []struct {
		name, contentType, errorField string
		keyIDs, signatures            []string
		status                        int
	}{
		{"missing key ID", signedRequestMediaType, "Formance-Signing-Key-Id", nil, []string{validSignature}, 400},
		{"empty key ID", signedRequestMediaType, "Formance-Signing-Key-Id", []string{""}, []string{validSignature}, 400},
		{"duplicate key ID", signedRequestMediaType, "Formance-Signing-Key-Id", []string{"key", "other"}, []string{validSignature}, 400},
		{"padded key ID", signedRequestMediaType, "Formance-Signing-Key-Id", []string{" key"}, []string{validSignature}, 400},
		{"missing signature", signedRequestMediaType, "Formance-Signature", []string{"key"}, nil, 400},
		{"empty signature", signedRequestMediaType, "Formance-Signature", []string{"key"}, []string{""}, 400},
		{"duplicate signature", signedRequestMediaType, "Formance-Signature", []string{"key"}, []string{validSignature, validSignature}, 400},
		{"joined signatures", signedRequestMediaType, "Formance-Signature", []string{"key"}, []string{validSignature + "," + validSignature}, 400},
		{"invalid signature base64", signedRequestMediaType, "Formance-Signature", []string{"key"}, []string{"!"}, 400},
		{"short signature", signedRequestMediaType, "Formance-Signature", []string{"key"}, []string{"AQ=="}, 400},
		{"oversized signature", signedRequestMediaType, "Formance-Signature", []string{"key"}, []string{base64.StdEncoding.EncodeToString(make([]byte, ed25519.SignatureSize+1))}, 400},
		{"signature on non-JSON", "text/plain", "", []string{"key"}, []string{validSignature}, 415},
		{"only key ID on JSON", "application/json", "Formance-Signature", []string{"key"}, nil, 400},
		{"only signature on JSON", "application/json", "Formance-Signing-Key-Id", nil, []string{validSignature}, 400},
		{"signature without content type", "", "", nil, []string{validSignature}, 415},
		{"former vendor media type", "application/vnd.formance.signed-request+json", "", []string{"key"}, []string{validSignature}, 415},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
			r := httptest.NewRequest(http.MethodPost, "/v3/ledger/transactions", strings.NewReader(`{"request":{},"payload":"AQ=="}`))
			r.Header.Set("Content-Type", tc.contentType)
			for _, value := range tc.keyIDs {
				r.Header.Add("Formance-Signing-Key-Id", value)
			}
			for _, value := range tc.signatures {
				r.Header.Add("Formance-Signature", value)
			}
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			if tc.errorField != "" {
				require.Contains(t, w.Body.String(), tc.errorField)
			}
		})
	}
}
