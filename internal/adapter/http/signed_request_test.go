package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
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
	envelope, err := protojson.Marshal(signed)
	require.NoError(t, err)
	wrapper, err := json.Marshal(struct {
		Request     json.RawMessage `json:"request"`
		SignedBatch json.RawMessage `json:"signedBatch"`
	}{body, envelope})
	require.NoError(t, err)
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, actual *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
		require.True(t, proto.Equal(signed, actual.GetSigned()), "HTTP must forward original client envelope unchanged")

		return &domain.ApplyResult{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1}}}}}}}}}}}, nil
	})
	router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
	r := httptest.NewRequest(http.MethodPost, "/v3/ledger/transactions", bytes.NewReader(wrapper))
	r.Header.Set("Content-Type", "application/vnd.formance.signed-request+json")
	r.Header.Set("Idempotency-Key", "signed-key")
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
		{"missing request", "POST", "/v3/ledger/transactions", `{"signedBatch":{}}`, 400},
		{"missing envelope", "POST", "/v3/ledger/transactions", `{"request":{}}`, 400},
		{"null envelope", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":null}`, 400},
		{"outer array", "POST", "/v3/ledger/transactions", `[]`, 400},
		{"unknown outer field", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":{},"extra":1}`, 400},
		{"duplicate request", "POST", "/v3/ledger/transactions", `{"request":{},"request":{},"signedBatch":{}}`, 400},
		{"duplicate envelope", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":{},"signedBatch":{}}`, 400},
		{"unknown envelope field", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":{"unknown":1}}`, 400},
		{"duplicate envelope key", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":{"keyId":"a","keyId":"b"}}`, 400},
		{"invalid base64", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":{"payload":"!"}}`, 400},
		{"trailing JSON", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":{}} {}`, 400},
		{"truncated JSON", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":{}`, 400},
		{"delete body", "DELETE", "/v3/ledger/transactions/1/metadata/key", `{"request":{},"signedBatch":{}}`, 400},
		{"sequential bulk", "POST", "/v3/ledger/bulk", `{"request":[{}],"signedBatch":{}}`, 400},
		{"empty atomic bulk", "POST", "/v3/ledger/bulk?atomic=true", `{"request":[],"signedBatch":{}}`, 400},
		{"unsupported ledger create", "POST", "/v3/ledger", `{"request":{},"signedBatch":{}}`, 415},
		{"unsupported ledger delete", "DELETE", "/v3/ledger", `{"request":null,"signedBatch":{}}`, 415},
		{"unsupported account metadata", "DELETE", "/v3/ledger/accounts/bank/metadata/key", `{"request":null,"signedBatch":{}}`, 415},
		{"oversized suffix", "POST", "/v3/ledger/transactions", `{"request":{},"signedBatch":{}}` + strings.Repeat(" ", int(defaultMaxBodySize)), 413},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			r.Header.Set("Content-Type", "application/vnd.formance.signed-request+json; charset=utf-8")
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
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{Enabled: true, KeySet: keySet, Issuer: testAuthIssuer, Service: "ledger", ScopeMapping: internalauth.DefaultMapping("ledger")}, version.Info{})
			path := "/v3/ledger/transactions"
			if tc.name == "transaction writer cannot write metadata" {
				path += "/1/metadata"
			}
			r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{"request":{},"signedBatch":{}}`))
			r.Header.Set("Content-Type", "application/vnd.formance.signed-request+json")
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
