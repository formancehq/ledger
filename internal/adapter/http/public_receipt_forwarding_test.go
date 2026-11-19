package http

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	grpcadapter "github.com/formancehq/ledger/v3/internal/adapter/grpc"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/pkg/version"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/httpreceipt"
)

func TestPublicReceiptSignedRequestThroughGRPC(t *testing.T) {
	t.Parallel()
	for _, replayed := range []bool{false, true} {
		name := "fresh"
		if replayed {
			name = "replayed"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			body := []byte(`{"postings":[{"source":"world","destination":"bank","amount":100,"asset":"USD"}]}`)
			payload := &servicepb.CreateTransactionPayload{}
			require.NoError(t, json.Unmarshal(body, payload))
			request := &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{Ledger: "ledger", Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{CreateTransaction: payload}}}}}
			signed, err := signing.Sign(&servicepb.ApplyBatch{IdempotencyKey: "signed-key", Requests: []*servicepb.Request{request}}, "client-key", ed25519.NewKeyFromSeed(make([]byte, 32)))
			require.NoError(t, err)
			wrapper, err := json.Marshal(struct {
				Request json.RawMessage `json:"request"`
				Payload []byte          `json:"payload"`
			}{body, signed.GetPayload()})
			require.NoError(t, err)
			leader := NewMockBackend(gomock.NewController(t))
			leader.EXPECT().Apply(gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, actual *servicepb.ApplyRequest) (*domain.ApplyResult, error) {
				require.True(t, proto.Equal(signed, actual.GetSigned()), "client signature and exact payload must survive HTTP and gRPC forwarding")

				return &domain.ApplyResult{Logs: []*commonpb.Log{receiptTransactionLog(42)}, Replayed: replayed}, nil
			})
			listener := bufconn.Listen(1 << 20)
			server := grpc.NewServer()
			servicepb.RegisterBucketServiceServer(server, grpcadapter.NewBucketServiceServer(logging.Testing(), leader, nil, nil, nil, nil, nil, nil, 0, "", noop.NewMeterProvider(), nil, nil, version.Info{}))
			served := make(chan error, 1)
			go func() { served <- server.Serve(listener) }()
			t.Cleanup(func() { server.Stop(); require.NoError(t, listener.Close()); require.NoError(t, <-served) })
			conn, err := grpc.NewClient("passthrough:///receipt-leader", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, conn.Close()) })
			// This is the same generated client and forwarding adapter selected by
			// RoutedController.getLeaderCtrl for a remote leader. Exercise real gRPC
			// serialization and server-produced trailers rather than mocking provenance.
			forwarded := grpcadapter.NewLedgerGrpcClient(servicepb.NewBucketServiceClient(conn))
			signer := signing.NewResponseSigner(bytes.Repeat([]byte{1}, 32))
			router := NewHandler(logging.Testing(), &DefaultBackend{Controller: forwarded}, internalauth.AuthConfig{}, version.Info{}, nil, signer)
			target := "/v3/ledger/transactions"
			req := httptest.NewRequest(http.MethodPost, target, bytes.NewReader(wrapper))
			ctx, cancel := context.WithTimeout(req.Context(), 5*time.Second)
			defer cancel()
			req = req.WithContext(ctx)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Idempotency-Key", "signed-key")
			req.Header.Set("Formance-Signing-Key-Id", signed.GetKeyId())
			req.Header.Set("Formance-Signature", base64.StdEncoding.EncodeToString(signed.GetSignature()))
			req.Header.Set(httpreceipt.Header, httpreceipt.Version)
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			require.Equal(t, http.StatusCreated, response.Code, response.Body.String())
			claims, err := httpreceipt.Verify(response.Header().Get(httpreceipt.Header), signer.PublicKey(), http.MethodPost, target, response.Code, response.Body.Bytes())
			require.NoError(t, err)
			require.EqualValues(t, 1, claims.Applies)
			require.EqualValues(t, 1, claims.Logs)
			if replayed {
				require.EqualValues(t, 1, claims.Replayed)
			} else {
				require.Zero(t, claims.Replayed)
			}
		})
	}
}

func TestPublicReceiptSignedRequestKeepsAuthorization(t *testing.T) {
	t.Parallel()
	key, keySet := testKeyPair(t)
	for _, test := range []struct {
		name, token, path string
		status            int
	}{
		{"anonymous", "", "/v3/ledger/transactions", 401},
		{"read only", signToken(t, key, testClaims("ledger:ReadTransaction")), "/v3/ledger/transactions", 403},
		{"writer cannot write metadata", signToken(t, key, testClaims("ledger:WriteTransaction")), "/v3/ledger/transactions/1/metadata", 403},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			backend := NewMockBackend(gomock.NewController(t)) // No Apply is permitted.
			signer := signing.NewResponseSigner(make([]byte, 32))
			router := NewHandler(logging.Testing(), backend, internalauth.AuthConfig{Enabled: true, KeySet: keySet, Issuers: []string{testAuthIssuer}, Service: "ledger", ScopeMapping: internalauth.DefaultMapping("ledger")}, version.Info{}, nil, signer)
			req := httptest.NewRequest(http.MethodPost, test.path, bytes.NewBufferString(`{"request":{},"payload":"AQ=="}`))
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Formance-Signing-Key-Id", "key")
			req.Header.Set("Formance-Signature", base64.StdEncoding.EncodeToString(make([]byte, 64)))
			req.Header.Set(httpreceipt.Header, httpreceipt.Version)
			if test.token != "" {
				req.Header.Set("Authorization", "Bearer "+test.token)
			}
			response := httptest.NewRecorder()
			router.ServeHTTP(response, req)
			require.Equal(t, test.status, response.Code, response.Body.String())
		})
	}
}
