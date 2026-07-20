//go:build e2e

package cluster

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	jose "github.com/go-jose/go-jose/v4"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// Dedicated port range for auth tests.
const (
	authTestHTTPPort = 15800
	authTestGRPCPort = 15900
	authTestRaftPort = 14800
)

// mockOIDCServer creates an HTTP test server that serves OIDC discovery and JWKS endpoints.
func mockOIDCServer(publicKey *rsa.PublicKey) *httptest.Server {
	jwk := jose.JSONWebKey{
		Key:       publicKey,
		KeyID:     "test-key-id",
		Algorithm: string(jose.RS256),
		Use:       "sig",
	}

	jwks := struct {
		Keys []jose.JSONWebKey `json:"keys"`
	}{
		Keys: []jose.JSONWebKey{jwk},
	}

	mux := http.NewServeMux()

	mux.HandleFunc("/jwks", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(jwks)
	})

	server := httptest.NewServer(mux)

	// Add discovery endpoint after server is started (needs base URL)
	discovery := map[string]any{
		"issuer":   server.URL,
		"jwks_uri": server.URL + "/jwks",
	}

	mux.HandleFunc("/.well-known/openid-configuration", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(discovery)
	})

	return server
}

// signJWT creates a signed JWT token with the given claims.
func signJWT(privKey *rsa.PrivateKey, claims *oidc.AccessTokenClaims) (string, error) {
	signer, err := jose.NewSigner(jose.SigningKey{
		Algorithm: jose.RS256,
		Key:       &jose.JSONWebKey{Key: privKey, KeyID: "test-key-id"},
	}, nil)
	if err != nil {
		return "", err
	}

	payload, err := json.Marshal(claims)
	if err != nil {
		return "", err
	}

	jws, err := signer.Sign(payload)
	if err != nil {
		return "", err
	}

	return jws.CompactSerialize()
}

// makeAuthClaims creates AccessTokenClaims with the given scopes.
func makeAuthClaims(issuer string, scopes ...string) *oidc.AccessTokenClaims {
	now := time.Now()
	claims := &oidc.AccessTokenClaims{}
	claims.Issuer = issuer
	claims.Subject = "test-user"
	claims.IssuedAt = oidc.FromTime(oidc.Time(now.Unix()).AsTime())
	claims.Expiration = oidc.FromTime(oidc.Time(now.Add(1 * time.Hour).Unix()).AsTime())
	claims.Scopes = oidc.SpaceDelimitedArray(scopes)
	return claims
}

// withAuthToken attaches a Bearer token to gRPC context metadata.
func withAuthToken(ctx context.Context, token string) context.Context {
	return metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)
}

var _ = Describe("Auth", Ordered, func() {
	var (
		ctx           context.Context
		grpcConn      *grpc.ClientConn
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
		privKey       *rsa.PrivateKey
		oidcServer    *httptest.Server
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

		// Generate RSA key pair for signing JWTs
		var err error
		privKey, err = rsa.GenerateKey(rand.Reader, 2048)
		Expect(err).To(Succeed())

		// Start mock OIDC server
		oidcServer = mockOIDCServer(&privKey.PublicKey)
		DeferCleanup(oidcServer.Close)

		// Start ledger server with auth enabled
		walTmpDir := GinkgoT().TempDir()
		dataTmpDir := GinkgoT().TempDir()
		DeferCleanup(func() {
			Expect(os.RemoveAll(walTmpDir)).To(Succeed())
			Expect(os.RemoveAll(dataTmpDir)).To(Succeed())
		})

		// Auth requires TLS (bearer tokens must not travel in plaintext), so
		// generate a throwaway CA + server cert for the fixture.
		certDir := GinkgoT().TempDir()
		certs, err := testserver.GenerateTestCerts(certDir)
		Expect(err).To(Succeed())

		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    1,
			ClusterID: "test-cluster",
			HTTPPort:  authTestHTTPPort,
			RaftPort:  authTestRaftPort,
			GRPCPort:  authTestGRPCPort,
			WalDir:    walTmpDir,
			DataDir:   dataTmpDir,
			Debug:     testutil.Debug,
			Output:    GinkgoWriter,
		})
		instruments = append(instruments,
			testserver.WithBootstrap(),
			testserver.WithAuthEnabled(),
			testserver.WithAuthIssuer(oidcServer.URL),
			testserver.WithAuthService("ledger"),
			testserver.WithTLSMode("required"),
			testserver.WithTLSCertFile(certs.ServerCertFile),
			testserver.WithTLSKeyFile(certs.ServerKeyFile),
		)

		server := testservice.New(cmdserver.NewRunCommand,
			testservice.WithInstruments(instruments...),
		)
		Expect(server.Start(ctx)).To(Succeed())

		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		// Auth requires TLS: dial the server over TLS trusting the fixture CA.
		client, clusterClient, grpcConn, err = newTLSGRPCClient(authTestGRPCPort, certs.CACertFile)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = grpcConn.Close() })

		// Wait for leader election
		Eventually(func(g Gomega) bool {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:admin"))
			g.Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			state, err := clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			return state.Leader != 0
		}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(BeTrue())
	})

	Context("with auth enabled", func() {
		It("should allow health check without token", func() {
			// Health check via gRPC health service is unauthenticated
			// We test Discovery RPC which is also unauthenticated
			resp, err := client.Discovery(ctx, &servicepb.DiscoveryRequest{})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should reject authenticated endpoints without token", func() {
			_, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: "test"})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should accept requests with valid token and correct scope", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:write"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			resp, err := client.Apply(authCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("auth-test-ledger", nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should reject requests with wrong scope", func() {
			// Token only has read scope, but Apply requires write
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:read"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			_, err = client.Apply(authCtx, servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("auth-test-ledger-2", nil)))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should allow read operations with read scope", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:read"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			// GetLedger (may return not-found, but shouldn't be auth error)
			_, err = client.GetLedger(authCtx, &servicepb.GetLedgerRequest{Ledger: "auth-test-ledger"})
			Expect(err).To(Succeed())
		})

		It("should allow write operations to create transactions", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:write"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			resp, err := client.Apply(authCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction("auth-test-ledger", []*commonpb.Posting{
				actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should allow admin operations with write scope", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:write"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			resp, err := client.Apply(authCtx, servicepb.UnsignedApplyRequest("", &servicepb.Request{
				Type: &servicepb.Request_SetMaintenanceMode{
					SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
						Enabled: false,
					},
				},
			}))
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should reject cluster operations without admin scope", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:read"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			_, err = clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.PermissionDenied))
		})

		It("should allow cluster operations with admin scope", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:admin"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			state, err := clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(Succeed())
			Expect(state.Leader).NotTo(BeZero())
		})

		It("should reject GetNumscript without token", func() {
			_, err := client.GetNumscript(ctx, &servicepb.GetNumscriptRequest{
				Ledger: "auth-test-ledger",
				Name:   "missing-script",
			})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should reject ListNumscripts without token", func() {
			stream, err := client.ListNumscripts(ctx, &servicepb.ListNumscriptsRequest{
				Ledger: "auth-test-ledger",
			})
			Expect(err).To(Succeed())
			_, err = stream.Recv()
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("should allow GetNumscript with ledger:read scope (ledger:QueryRead mapped)", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:read"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			// Script doesn't exist; NotFound is the expected non-auth outcome.
			_, err = client.GetNumscript(authCtx, &servicepb.GetNumscriptRequest{
				Ledger: "auth-test-ledger",
				Name:   "missing-script",
			})
			if err != nil {
				st, ok := status.FromError(err)
				Expect(ok).To(BeTrue())
				Expect(st.Code()).NotTo(Equal(codes.Unauthenticated))
				Expect(st.Code()).NotTo(Equal(codes.PermissionDenied))
			}
		})

		It("should allow ListNumscripts with ledger:read scope (ledger:QueryRead mapped)", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:read"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			stream, err := client.ListNumscripts(authCtx, &servicepb.ListNumscriptsRequest{
				Ledger: "auth-test-ledger",
			})
			Expect(err).To(Succeed())
			// Drain the stream; empty ledger means the stream ends with EOF.
			for {
				_, err := stream.Recv()
				if err != nil {
					st, ok := status.FromError(err)
					if !ok {
						break // io.EOF
					}

					Expect(st.Code()).NotTo(Equal(codes.Unauthenticated))
					Expect(st.Code()).NotTo(Equal(codes.PermissionDenied))

					break
				}
			}
		})
	})
})
