package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"fmt"
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
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestSignedHTTPRoutes_ForwardOriginalProof(t *testing.T) {
	t.Parallel()
	body := []byte("\n " + `{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD"}]}` + " \n")
	proof := &signaturepb.SignedHTTPRequest{Method: "POST", Authority: "example.com", RequestTarget: "/v3/ledger/transactions", ContentType: "application/json", IdempotencyKey: "signed-key", Body: body}
	require.NoError(t, signing.SignHTTP(proof, "key", ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))))
	backend := NewMockBackend(gomock.NewController(t))
	backend.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, actual *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
		require.True(t, proto.Equal(proof, actual.GetHttpSigned()), "HTTP must forward original client HTTP proof unchanged")

		return &domain.ApplyResult{Logs: []*commonpb.Log{{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 1}}}}}}}}}}}, nil
	})
	router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
	r := httptest.NewRequest(http.MethodPost, "/v3/ledger/transactions", bytes.NewReader(body))
	setSignedHTTPHeaders(r, proof)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, r)
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())
}

func setSignedHTTPHeaders(r *http.Request, proof *signaturepb.SignedHTTPRequest) {
	r.Header.Set("Content-Type", proof.GetContentType())
	r.Header.Set("Content-Digest", proof.GetContentDigest())
	r.Header.Set("Idempotency-Key", proof.GetIdempotencyKey())
	r.Header.Set("Signature-Input", proof.GetSignatureInput())
	r.Header.Set("Signature", proof.GetSignature())
}

func TestSignedHTTPRoutes_RejectMalformedHeaders(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"Content-Type", "Content-Digest", "Idempotency-Key", "Signature-Input", "Signature"} {
		for _, duplicate := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s duplicate=%t", name, duplicate), func(t *testing.T) {
				t.Parallel()
				backend := NewMockBackend(gomock.NewController(t))
				router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
				r := httptest.NewRequest(http.MethodPost, "/v3/ledger/transactions", strings.NewReader(`{}`))
				setSignedHTTPHeaders(r, &signaturepb.SignedHTTPRequest{ContentType: "application/json", ContentDigest: "digest", SignatureInput: "input", Signature: "sig"})
				if duplicate {
					r.Header.Add(name, "duplicate")
				} else {
					r.Header.Del(name)
				}
				w := httptest.NewRecorder()
				router.ServeHTTP(w, r)
				require.Equal(t, 400, w.Code, w.Body.String())
			})
		}
	}
}

func TestSignedHTTPRoutes_RejectUnsupportedOrInvalidBodies(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, method, path, body string
		status                   int
	}{
		{"delete body", "DELETE", "/v3/ledger/transactions/1/metadata/key", `{}`, 400},
		{"trailing JSON", "POST", "/v3/ledger/transactions", `{} {}`, 400},
		{"truncated JSON", "POST", "/v3/ledger/transactions", `{"postings":`, 400},
		{"trailing non-whitespace", "POST", "/v3/ledger/transactions", `{} invalid`, 400},
		{"sequential bulk", "POST", "/v3/ledger/bulk", `[{}]`, 400},
		{"empty atomic bulk", "POST", "/v3/ledger/bulk?atomic=true", `[]`, 400},
		{"unsupported ledger create", "POST", "/v3/ledger", `{}`, 415},
		{"unsupported ledger delete", "DELETE", "/v3/ledger", ``, 415},
		{"unsupported account metadata", "DELETE", "/v3/ledger/accounts/bank/metadata/key", ``, 415},
		{"oversized suffix", "POST", "/v3/ledger/transactions", `{}` + strings.Repeat(" ", int(defaultMaxBodySize)), 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t))
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{}, version.Info{})
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			proof := &signaturepb.SignedHTTPRequest{Method: tc.method, RequestTarget: tc.path, Authority: r.Host, ContentType: "application/json", Body: []byte(tc.body)}
			if tc.status == http.StatusRequestEntityTooLarge {
				// Body capture must fail before cryptographic admission, even when
				// the trailing bytes exceed the complete signed-body budget.
				proof.ContentDigest, proof.SignatureInput, proof.Signature = "digest", "input", "signature"
			} else {
				require.NoError(t, signing.SignHTTP(proof, "key", ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))))
			}
			setSignedHTTPHeaders(r, proof)
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
			r := httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))
			r.Header.Set("Signature", "invalid")
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

func TestCaptureSignedRequest_Scheme(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, target, scheme string
		tls                  bool
	}{
		{"origin HTTP", "/v3/ledger/transactions", "http", false},
		{"origin HTTPS", "/v3/ledger/transactions", "https", true},
		{"absolute HTTPS", "https://example.com/v3/ledger/transactions", "https", false},
		{"explicit URL scheme", "http://example.com/v3/ledger/transactions", "http", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			r := httptest.NewRequest(http.MethodPost, tc.target, strings.NewReader(`{}`))
			if tc.tls {
				r.TLS = &tls.ConnectionState{}
			}
			// Untrusted forwarding headers must never select the signed scheme.
			r.Header.Set("X-Forwarded-Proto", "unexpected")
			setSignedHTTPHeaders(r, &signaturepb.SignedHTTPRequest{ContentType: "application/json", ContentDigest: "digest", SignatureInput: "input", Signature: "sig"})
			proof, err := captureSignedRequest(r)
			require.NoError(t, err)
			require.Equal(t, tc.scheme, proof.GetScheme())
		})
	}
}
