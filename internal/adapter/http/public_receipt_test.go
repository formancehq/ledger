package http

import (
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/httpreceipt"
)

func TestAtomicApplyPublicProofAuthenticatesRedactedOutcome(t *testing.T) {
	t.Parallel()
	signer := signing.NewResponseSigner(make([]byte, ed25519.SeedSize))
	const secret = "reusable-credential-value"
	service := atomicApplyTestService{apply: func(*servicepb.ApplyRequest) (*servicepb.ApplyResponse, error) {
		config := &commonpb.SinkConfig{Name: "webhook", Type: &commonpb.SinkConfig_Http{Http: &commonpb.HttpSinkConfig{Endpoint: "https://example.com/events", Secret: secret}}}
		log := &commonpb.Log{Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: config}}}}
		log.ResponseSignature = signer.SignLog(log)

		return &servicepb.ApplyResponse{Logs: []*commonpb.Log{log}}, nil
	}}
	handler := NewHandler(logging.Testing(), NewMockBackend(gomock.NewController(t)), internalauth.AuthConfig{}, version.Info{}, service, signer)
	request := httptest.NewRequest(http.MethodPost, "/v3/_/apply", strings.NewReader(`{"requests":[{"createLedger":{"name":"one"}}]}`))
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set(httpreceipt.Header, httpreceipt.Version)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusOK, response.Code, response.Body.String())
	_, err := httpreceipt.Verify(response.Header().Get(httpreceipt.Header), signer.PublicKey(), http.MethodPost, "/v3/_/apply", response.Code, response.Body.Bytes())
	require.NoError(t, err)
	require.Contains(t, response.Body.String(), `"sequence":7`)
	require.NotContains(t, response.Body.String(), secret)
	require.NotContains(t, response.Body.String(), "responseSignature")

	tampered := append([]byte(nil), response.Body.Bytes()...)
	tampered[0] ^= 1
	_, err = httpreceipt.Verify(response.Header().Get(httpreceipt.Header), signer.PublicKey(), http.MethodPost, "/v3/_/apply", response.Code, tampered)
	require.Error(t, err)
}

func TestAtomicApplyProofRequiresSignerBeforeDispatch(t *testing.T) {
	t.Parallel()
	handler := NewHandler(logging.Testing(), NewMockBackend(gomock.NewController(t)), internalauth.AuthConfig{}, version.Info{}, nil)
	request := httptest.NewRequest(http.MethodPost, "/v3/_/apply", strings.NewReader(`{"requests":[{"createLedger":{"name":"one"}}]}`))
	request.Header.Set(httpreceipt.Header, httpreceipt.Version)
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	require.Equal(t, http.StatusServiceUnavailable, response.Code)
}
