//go:build e2e

package cluster

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"encoding/json"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/authn/oidc"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	jose "github.com/go-jose/go-jose/v4"
	cmdserver "github.com/formancehq/ledger-v3-poc/cmd/server"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
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

		server := testservice.New(cmdserver.NewRunCommand,
			testservice.WithInstruments(
				testservice.DebugInstrumentation(testutil.Debug),
				testservice.OutputInstrumentation(GinkgoWriter),
				testserver.WithNodeID(1),
				testserver.WithClusterID("test-cluster"),
				testserver.WithHTTPPort(authTestHTTPPort),
				testserver.WithWalDir(walTmpDir),
				testserver.WithDataDir(dataTmpDir),
				testserver.WithRaftPort(authTestRaftPort),
				testserver.WithGRPCPort(authTestGRPCPort),
				testserver.WithSnapshotThreshold(10),
				testserver.WithDebug(os.Getenv("DEBUG") == "true"),
				testserver.WithRaftTickInterval(10*time.Millisecond),
				testserver.WithRaftHeartbeatTick(1),
				testserver.WithRaftElectionTick(10),
				testserver.WithBootstrap(),
				// Auth configuration
				testserver.WithAuthEnabled(),
				testserver.WithAuthIssuer(oidcServer.URL),
				testserver.WithAuthService("ledger"),
			),
		)
		Expect(server.Start(ctx)).To(Succeed())

		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		// Create insecure gRPC client (no TLS needed for this test)
		grpcConn, err = grpc.NewClient(
			fmt.Sprintf("localhost:%d", authTestGRPCPort),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithDefaultServiceConfig(testutil.GRPCRetryPolicy),
		)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = grpcConn.Close() })

		client = servicepb.NewBucketServiceClient(grpcConn)
		clusterClient = clusterpb.NewClusterServiceClient(grpcConn)

		// Wait for leader election
		Eventually(func(g Gomega) bool {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:admin"))
			g.Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			state, err := clusterClient.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			return state.Leader != 0
		}).Within(10 * time.Second).ProbeEvery(200 * time.Millisecond).Should(BeTrue())
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

			resp, err := client.Apply(authCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction("auth-test-ledger", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should reject requests with wrong scope", func() {
			// Token only has read scope, but Apply requires write
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:read"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			_, err = client.Apply(authCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateLedgerAction("auth-test-ledger-2", nil),
				},
			})
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

			resp, err := client.Apply(authCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					testutil.CreateTransactionAction("auth-test-ledger", []*commonpb.Posting{
						testutil.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should allow admin operations with write scope", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:write"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			resp, err := client.Apply(authCtx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					{
						Type: &servicepb.Request_SetMaintenanceMode{
							SetMaintenanceMode: &servicepb.SetMaintenanceModeRequest{
								Enabled: false,
							},
						},
					},
				},
			})
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
	})
})
