//go:build e2e

package cluster

import (
	"context"
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"

	cmdserver "github.com/formancehq/ledger/v3/cmd/server"
	"github.com/formancehq/ledger/v3/internal/proto/clusterpb"
	"github.com/formancehq/ledger/v3/pkg/grpcprotocol"
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// Pins the expected behavior of a ledgerctl-style TLS client (no client cert,
// operator Credentials JWT) against a tls-mode=required server with CAFile set
// (mTLS opt-in) — the exact transport shape the operator-driven backup Job and
// raft scale-down ledgerctl invocations use. The RPC must succeed; if a
// regression ever makes this start failing with "error reading server preface",
// we want to catch it in CI instead of in production.
var _ = Describe("ledgerctl TLS bearer against tls-mode=required", Ordered, func() {
	var (
		certs         *testserver.TestCerts
		lease         *testserver.NodeLease
		ports         testserver.NodePorts
		operatorPriv  ed25519.PrivateKey
		operatorKeyID = "operator-key"
	)

	BeforeAll(func() {
		ctx := logging.TestingContext()
		certDir := GinkgoT().TempDir()
		var authKeysPath string
		var err error
		operatorPriv, authKeysPath, err = writeEd25519KeysConfig(certDir, operatorKeyID, []string{
			"ledger:ReadOps", "ledger:WriteOps", "ledger:ReadCluster", "ledger:WriteCluster",
		})
		Expect(err).To(Succeed())
		certs, err = testserver.GenerateTestCerts(certDir)
		Expect(err).To(Succeed())

		walTmpDir := GinkgoT().TempDir()
		dataTmpDir := GinkgoT().TempDir()
		DeferCleanup(func() {
			_ = os.RemoveAll(walTmpDir)
			_ = os.RemoveAll(dataTmpDir)
		})

		lease = testserver.AllocateNodeLease()
		ports = lease.Ports()

		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    1,
			ClusterID: "test-cluster",
			Ports:     ports,
			WalDir:    walTmpDir,
			DataDir:   dataTmpDir,
			Debug:     testutil.Debug,
			Output:    GinkgoWriter,
		})
		instruments = append(instruments,
			testserver.WithBootstrap(),
			testserver.WithTLSMode("required"),
			testserver.WithTLSCertFile(certs.ServerCertFile),
			testserver.WithTLSKeyFile(certs.ServerKeyFile),
			testserver.WithTLSCACertFile(certs.CACertFile), // mTLS opt-in, matches operator setup
			testservice.InstrumentationFunc(func(ctx context.Context, cfg *testservice.RunConfiguration) error {
				cfg.AppendArgs("--auth-enabled", "--auth-ed25519-keys", authKeysPath, "--auth-service", "ledger")
				return nil
			}),
		)

		server := lease.NewService(cmdserver.NewRunCommandWithBindings,
			testservice.WithInstruments(instruments...),
		)
		Expect(server.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			_ = server.Stop(ctx)
		})
	})

	It("connects with TLS + operator Credentials JWT and gets a successful RPC", func() {
		caPEM, err := os.ReadFile(certs.CACertFile)
		Expect(err).To(Succeed())
		pool := x509.NewCertPool()
		Expect(pool.AppendCertsFromPEM(caPEM)).To(BeTrue())

		tlsCfg := &tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		}
		addr := fmt.Sprintf("localhost:%d", ports.GRPC())
		conn, err := grpc.NewClient(addr,
			grpcprotocol.ClientOption(),
			grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = conn.Close() })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		token, err := signEdDSAJWT(operatorPriv, operatorKeyID, makeEdDSAClaims(
			"ledger:ReadOps", "ledger:WriteOps", "ledger:ReadCluster", "ledger:WriteCluster",
		))
		Expect(err).To(Succeed())
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+token)

		c := clusterpb.NewClusterServiceClient(conn)
		Eventually(func(g Gomega) {
			state, err := c.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed(),
				"RPC must succeed; if you see 'error reading server preface' here, the production bug is reproduced")
			g.Expect(state.Leader).NotTo(BeZero())
		}).Within(10 * time.Second).Should(Succeed())
	})
})
