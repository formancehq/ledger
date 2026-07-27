//go:build e2e

package cluster

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Dedicated port range for writes-only auth tests.
const (
	writesOnlyHTTPPort = 15810
	writesOnlyGRPCPort = 15910
	writesOnlyRaftPort = 14810
)

var _ = Describe("Auth writes-only mode", Ordered, func() {
	var (
		ctx        context.Context
		grpcConn   *grpc.ClientConn
		client     servicepb.BucketServiceClient
		clusterCli clusterpb.ClusterServiceClient
		privKey    *rsa.PrivateKey
		oidcServer *httptest.Server
		httpAddr   string
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

		// Generate RSA keypair + mock OIDC discovery server (re-uses helpers
		// defined in auth_test.go: signJWT, makeAuthClaims, mockOIDCServer,
		// withAuthToken).
		var err error
		privKey, err = rsa.GenerateKey(rand.Reader, 2048)
		Expect(err).To(Succeed())

		oidcServer = mockOIDCServer(&privKey.PublicKey)
		DeferCleanup(oidcServer.Close)

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
			HTTPPort:  writesOnlyHTTPPort,
			RaftPort:  writesOnlyRaftPort,
			GRPCPort:  writesOnlyGRPCPort,
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
			// Writes-only: every read scope is granted to anonymous callers.
			testserver.WithAuthAnonymousScopes("*:read"),
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
		client, clusterCli, grpcConn, err = newTLSGRPCClient(writesOnlyGRPCPort, certs.CACertFile)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = grpcConn.Close() })
		httpAddr = fmt.Sprintf("http://localhost:%d", writesOnlyHTTPPort)

		// Wait for leader election (use a write-scoped token).
		Eventually(func(g Gomega) bool {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:admin"))
			g.Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			state, err := clusterCli.GetClusterState(authCtx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			return state.Leader != 0
		}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(BeTrue())

		// Seed a ledger that the read tests can target.
		writeToken, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:write"))
		Expect(err).To(Succeed())
		_, err = client.Apply(withAuthToken(ctx, writeToken), servicepb.UnsignedApplyRequest("", actions.CreateLedgerAction("writes-only-ledger", nil)))
		Expect(err).To(Succeed())
	})

	Context("gRPC", func() {
		It("allows reads without a token (anonymous grants *:read)", func() {
			_, err := client.GetLedger(ctx, &servicepb.GetLedgerRequest{Ledger: "writes-only-ledger"})
			Expect(err).To(Succeed())
		})

		It("rejects reads with an invalid token (token errors are not swallowed)", func() {
			authCtx := withAuthToken(ctx, "this-is-not-a-jwt")
			_, err := client.GetLedger(authCtx, &servicepb.GetLedgerRequest{Ledger: "writes-only-ledger"})
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("rejects writes without a token", func() {
			_, err := client.Apply(ctx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction("writes-only-ledger", []*commonpb.Posting{
				actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(HaveOccurred())
			st, ok := status.FromError(err)
			Expect(ok).To(BeTrue())
			Expect(st.Code()).To(Equal(codes.Unauthenticated))
		})

		It("allows writes with a valid token carrying ledger:write", func() {
			token, err := signJWT(privKey, makeAuthClaims(oidcServer.URL, "ledger:write"))
			Expect(err).To(Succeed())
			authCtx := withAuthToken(ctx, token)

			_, err = client.Apply(authCtx, servicepb.UnsignedApplyRequest("", actions.CreateTransactionAction("writes-only-ledger", []*commonpb.Posting{
				actions.NewPosting("world", "bank", big.NewInt(100), "USD"),
			}, nil, nil)))
			Expect(err).To(Succeed())
		})
	})

	Context("HTTP", func() {
		It("allows reads without a token", func() {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpAddr+"/v3/writes-only-ledger", nil)
			Expect(err).To(Succeed())

			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(Succeed())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusOK))
		})

		It("rejects reads with a malformed token", func() {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, httpAddr+"/v3/writes-only-ledger", nil)
			Expect(err).To(Succeed())
			req.Header.Set("Authorization", "Bearer not-a-real-jwt")

			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(Succeed())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
		})

		It("rejects writes without a token", func() {
			req, err := http.NewRequestWithContext(ctx, http.MethodPost, httpAddr+"/v3/new-ledger", nil)
			Expect(err).To(Succeed())

			resp, err := http.DefaultClient.Do(req)
			Expect(err).To(Succeed())
			defer resp.Body.Close()

			Expect(resp.StatusCode).To(Equal(http.StatusUnauthorized))
		})
	})
})
