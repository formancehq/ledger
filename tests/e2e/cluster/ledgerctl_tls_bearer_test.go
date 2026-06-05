//go:build e2e

package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
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
	"github.com/formancehq/ledger/v3/pkg/testserver"
	"github.com/formancehq/ledger/v3/tests/e2e/testutil"
)

// Pins the expected behavior of a ledgerctl-style TLS client (no client cert,
// cluster-secret bearer) against a tls-mode=required server with CAFile set
// (mTLS opt-in) — the exact transport shape the operator-driven backup Job and
// raft scale-down ledgerctl invocations use. The RPC must succeed; if a
// regression ever makes this start failing with "error reading server preface",
// we want to catch it in CI instead of in production.
var _ = Describe("ledgerctl TLS bearer against tls-mode=required", Ordered, func() {
	var (
		certs      *testserver.TestCerts
		grpcPort   int
		clusterTok = "test-cluster-secret-840bef"
	)

	BeforeAll(func() {
		ctx := logging.TestingContext()
		certDir := GinkgoT().TempDir()
		var err error
		certs, err = testserver.GenerateTestCerts(certDir)
		Expect(err).To(Succeed())

		walTmpDir := GinkgoT().TempDir()
		dataTmpDir := GinkgoT().TempDir()
		DeferCleanup(func() {
			_ = os.RemoveAll(walTmpDir)
			_ = os.RemoveAll(dataTmpDir)
		})

		grpcPort = freeTLSReproPort()
		raftPort := freeTLSReproPort()
		httpPort := freeTLSReproPort()

		instruments := testserver.DefaultTestInstruments(testserver.TestNodeConfig{
			NodeID:    1,
			ClusterID: "test-cluster",
			HTTPPort:  httpPort,
			RaftPort:  raftPort,
			GRPCPort:  grpcPort,
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
				cfg.AppendArgs("--cluster-secret", clusterTok)
				return nil
			}),
		)

		server := testservice.New(cmdserver.NewRunCommand,
			testservice.WithInstruments(instruments...),
		)
		Expect(server.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			_ = server.Stop(ctx)
		})
	})

	It("connects with TLS + cluster-secret bearer and gets a non-preface RPC error", func() {
		caPEM, err := os.ReadFile(certs.CACertFile)
		Expect(err).To(Succeed())
		pool := x509.NewCertPool()
		Expect(pool.AppendCertsFromPEM(caPEM)).To(BeTrue())

		tlsCfg := &tls.Config{
			RootCAs:    pool,
			MinVersion: tls.VersionTLS12,
		}
		addr := fmt.Sprintf("localhost:%d", grpcPort)
		conn, err := grpc.NewClient(addr,
			grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		)
		Expect(err).To(Succeed())
		DeferCleanup(func() { _ = conn.Close() })

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx, "authorization", "Bearer "+clusterTok)

		c := clusterpb.NewClusterServiceClient(conn)
		Eventually(func(g Gomega) {
			state, err := c.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed(),
				"RPC must succeed; if you see 'error reading server preface' here, the production bug is reproduced")
			g.Expect(state.Leader).NotTo(BeZero())
		}).Within(10 * time.Second).Should(Succeed())
	})
})

func freeTLSReproPort() int {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	Expect(err).To(Succeed())
	defer func() { _ = l.Close() }()
	return l.Addr().(*net.TCPAddr).Port
}
