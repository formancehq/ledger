//go:build e2e

package cluster

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"net/http/httptest"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Both issuers come only from --auth-issuers, the form the operator emits as
// AUTH_ISSUERS for spec.auth.issuers. Their discovery servers publish the same
// key ID, so a token is accepted only when the key set of the issuer it names
// verifies its signature.
var _ = Describe("Auth with plural OIDC issuers", Ordered, func() {
	var (
		ctx           context.Context
		grpcConn      *grpc.ClientConn
		clusterClient clusterpb.ClusterServiceClient
		keyA, keyB    *rsa.PrivateKey
		issuerA       *httptest.Server
		issuerB       *httptest.Server
	)

	BeforeAll(func() {
		ctx = logging.TestingContext()

		var err error
		keyA, err = rsa.GenerateKey(rand.Reader, 2048)
		Expect(err).To(Succeed())
		keyB, err = rsa.GenerateKey(rand.Reader, 2048)
		Expect(err).To(Succeed())

		issuerA = mockOIDCServer(&keyA.PublicKey)
		DeferCleanup(issuerA.Close)
		issuerB = mockOIDCServer(&keyB.PublicKey)
		DeferCleanup(issuerB.Close)

		certs, err := testserver.GenerateTestCerts(GinkgoT().TempDir())
		Expect(err).To(Succeed())

		lease := testserver.AllocateNodeLease()
		ports := lease.Ports()

		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    1,
			ClusterID: "test-cluster",
			Ports:     ports,
			WalDir:    GinkgoT().TempDir(),
			DataDir:   GinkgoT().TempDir(),
			Debug:     testutil.Debug,
			Output:    GinkgoWriter,
		})
		instruments = append(instruments,
			testserver.WithBootstrap(),
			testserver.WithAuthEnabled(),
			testserver.WithAuthIssuers(issuerA.URL, issuerB.URL),
			testserver.WithAuthService("ledger"),
			testserver.WithTLSMode("required"),
			testserver.WithTLSCertFile(certs.ServerCertFile),
			testserver.WithTLSKeyFile(certs.ServerKeyFile),
		)

		server := lease.NewService(cmdserver.NewRunCommandWithBindings,
			testservice.WithInstruments(instruments...),
		)
		Expect(server.Start(ctx)).To(Succeed())

		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(server.Stop(stopCtx)).To(Succeed())
		})

		_, clusterClient, grpcConn, err = newTLSGRPCClient(ports.GRPC(), certs.CACertFile)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = grpcConn.Close() })

		Eventually(func(g Gomega) bool {
			token, err := signJWT(keyA, makeAuthClaims(issuerA.URL, "ledger:admin"))
			g.Expect(err).To(Succeed())

			state, err := clusterClient.GetClusterState(withAuthToken(ctx, token), &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			return state.Leader != 0
		}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(BeTrue())
	})

	getClusterState := func(key *rsa.PrivateKey, issuer string) error {
		token, err := signJWT(key, makeAuthClaims(issuer, "ledger:admin"))
		Expect(err).To(Succeed())

		_, err = clusterClient.GetClusterState(withAuthToken(ctx, token), &clusterpb.GetClusterStateRequest{})
		return err
	}

	expectUnauthenticated := func(err error) {
		Expect(err).To(HaveOccurred())
		st, ok := status.FromError(err)
		Expect(ok).To(BeTrue())
		Expect(st.Code()).To(Equal(codes.Unauthenticated))
	}

	It("accepts a token from the first issuer", func() {
		Expect(getClusterState(keyA, issuerA.URL)).To(Succeed())
	})

	It("accepts a token from the second issuer", func() {
		Expect(getClusterState(keyB, issuerB.URL)).To(Succeed())
	})

	It("rejects a token naming one issuer but signed by the other's key", func() {
		expectUnauthenticated(getClusterState(keyA, issuerB.URL))
		expectUnauthenticated(getClusterState(keyB, issuerA.URL))
	})

	It("rejects a token from an unconfigured issuer", func() {
		expectUnauthenticated(getClusterState(keyA, "https://unconfigured.example.com"))
	})
})
