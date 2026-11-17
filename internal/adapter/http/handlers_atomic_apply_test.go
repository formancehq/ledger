package http

import (
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
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

type atomicApplyTestService struct {
	servicepb.UnimplementedBucketServiceServer

	apply func(*servicepb.ApplyRequest) (*servicepb.ApplyResponse, error)
}

func (s atomicApplyTestService) Apply(_ context.Context, request *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
	return s.apply(request)
}

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

func TestAtomicApplyHTTPSignedIntentBinding(t *testing.T) {
	t.Parallel()
	const key = "same-key"
	for _, tc := range []struct {
		name         string
		signedAmount uint64
		status       int
	}{
		{name: "matching", signedAmount: 1, status: http.StatusOK},
		{name: "valid posting mismatch", signedAmount: 2, status: http.StatusForbidden},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			visible := json.RawMessage(`{"requests":[{"apply":{"ledger":"one","action":{"createTransaction":{"postings":[{"source":"world","destination":"bank","amount":1,"asset":"USD"}]}}}}]}`)
			batch, err := decodeAtomicApplyBatch(visible)
			require.NoError(t, err)
			batch.IdempotencyKey = key
			batch.GetRequests()[0].GetApply().GetAction().GetCreateTransaction().Postings[0].Amount = commonpb.NewUint256FromUint64(tc.signedAmount)
			signed, err := signing.Sign(batch, "key", ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize)))
			require.NoError(t, err)
			body, err := json.Marshal(struct {
				Request json.RawMessage `json:"request"`
				Payload []byte          `json:"payload"`
			}{visible, signed.GetPayload()})
			require.NoError(t, err)
			called := false
			service := atomicApplyTestService{apply: func(request *servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
				called = true
				require.True(t, proto.Equal(signed, request.GetSigned()))

				return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "one"}}}}}}, nil
			}}
			router := NewHandler(logging.Testing(), NewMockBackend(gomock.NewController(t)), internalauth.AuthConfig{}, version.Info{}, service)
			r := httptest.NewRequest(http.MethodPost, "/v3/_/apply", strings.NewReader(string(body)))
			r.Header.Set("Content-Type", "application/json")
			r.Header.Set("Idempotency-Key", key)
			r.Header.Set("Formance-Signing-Key-Id", signed.GetKeyId())
			r.Header.Set("Formance-Signature", base64.StdEncoding.EncodeToString(signed.GetSignature()))
			w := httptest.NewRecorder()
			router.ServeHTTP(w, r)
			require.Equal(t, tc.status, w.Code, w.Body.String())
			require.Equal(t, tc.status == http.StatusOK, called)
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
