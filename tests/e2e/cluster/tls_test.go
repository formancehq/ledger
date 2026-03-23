//go:build e2e

package cluster

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"math/big"
	"os"
	"time"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/formancehq/go-libs/v5/pkg/testing/testservice"
	cmdserver "github.com/formancehq/ledger-v3-poc/cmd/server"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/actions"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"github.com/formancehq/ledger-v3-poc/tests/e2e/testutil"
)

// Dedicated port range for TLS tests to avoid conflict with other e2e tests.
const (
	// Single-node TLS test ports
	tlsTestHTTPPort = 15400
	tlsTestGRPCPort = 15500
	tlsTestRaftPort = 14500

	// Multi-node TLS test ports (up to 3 nodes: base+0, base+1, base+2)
	tlsMultiRaftBasePort    = 14600
	tlsMultiServiceBasePort = 15600
	tlsMultiHTTPBasePort    = 15700
)

// newTLSGRPCClient creates a gRPC client that uses TLS with the given CA cert file.
func newTLSGRPCClient(grpcPort int, caCertFile string) (servicepb.BucketServiceClient, clusterpb.ClusterServiceClient, *grpc.ClientConn, error) {
	caPEM, err := os.ReadFile(caCertFile)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("reading CA cert: %w", err)
	}

	certPool := x509.NewCertPool()
	if !certPool.AppendCertsFromPEM(caPEM) {
		return nil, nil, nil, fmt.Errorf("failed to parse CA cert")
	}

	tlsCfg := &tls.Config{
		RootCAs:    certPool,
		MinVersion: tls.VersionTLS12,
	}

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
		grpc.WithDefaultServiceConfig(actions.GRPCRetryPolicy),
	)
	if err != nil {
		return nil, nil, nil, err
	}

	return servicepb.NewBucketServiceClient(conn), clusterpb.NewClusterServiceClient(conn), conn, nil
}

// setupTLSSingleNode creates a single-node TLS-enabled server and returns the context,
// a TLS-capable gRPC client, and the generated certs for further use.
func setupTLSSingleNode(httpPort, grpcPort, raftPort int) (context.Context, servicepb.BucketServiceClient, clusterpb.ClusterServiceClient, *testserver.TestCerts) {
	ctx := logging.TestingContext()

	// Generate test certificates
	certDir := GinkgoT().TempDir()
	certs, err := testserver.GenerateTestCerts(certDir)
	Expect(err).To(Succeed())

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
			testserver.WithHTTPPort(httpPort),
			testserver.WithWalDir(walTmpDir),
			testserver.WithDataDir(dataTmpDir),
			testserver.WithRaftPort(raftPort),
			testserver.WithGRPCPort(grpcPort),
			testserver.WithSnapshotThreshold(10),
			testserver.WithDebug(os.Getenv("DEBUG") == "true"),
			testserver.WithRaftTickInterval(10*time.Millisecond),
			testserver.WithRaftHeartbeatTick(1),
			testserver.WithRaftElectionTick(10),
			testserver.WithBootstrap(),
			// TLS configuration
			testserver.WithTLSCertFile(certs.ServerCertFile),
			testserver.WithTLSKeyFile(certs.ServerKeyFile),
		),
	)
	Expect(server.Start(ctx)).To(Succeed())

	DeferCleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		Expect(server.Stop(stopCtx)).To(Succeed())
	})

	// Create TLS-enabled gRPC client using the CA cert
	grpcClient, clusterClient, grpcConn, err := newTLSGRPCClient(grpcPort, certs.CACertFile)
	Expect(err).To(Succeed())
	DeferCleanup(func() {
		_ = grpcConn.Close()
	})

	// Wait for leader election
	Eventually(func(g Gomega) bool {
		state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		g.Expect(err).To(Succeed())
		return state.Leader != 0
	}).Within(5 * time.Second).To(BeTrue())

	return ctx, grpcClient, clusterClient, certs
}

// tlsServiceWithClient extends serviceWithClient with TLS-specific fields.
type tlsServiceWithClient struct {
	service       *testservice.Service
	client        servicepb.BucketServiceClient
	clusterClient clusterpb.ClusterServiceClient
	grpcConn      *grpc.ClientConn
	grpcPort      int
	nodeID        uint32
}

// setupTLSMultiNodeCluster creates a multi-node Raft cluster with TLS enabled on all nodes.
// All nodes share the same TLS certificate (valid for localhost/127.0.0.1).
// Returns the context, the list of TLS services with clients, and the generated certs.
func setupTLSMultiNodeCluster(
	countInstances int,
	raftBasePort, serviceBasePort, httpBasePort int,
) (context.Context, []*tlsServiceWithClient, *testserver.TestCerts) {
	ctx := logging.TestingContext()

	// Generate shared test certificates for all nodes
	certDir := GinkgoT().TempDir()
	certs, err := testserver.GenerateTestCerts(certDir)
	Expect(err).To(Succeed())

	// Common instruments shared by all TLS nodes
	commonInstruments := func(i int, walDir, dataDir string) []testservice.Instrumentation {
		return []testservice.Instrumentation{
			testservice.DebugInstrumentation(testutil.Debug),
			testservice.OutputInstrumentation(GinkgoWriter),
			testserver.WithNodeID(i + 1),
			testserver.WithClusterID("test-cluster"),
			testserver.WithHTTPPort(httpBasePort + i),
			testserver.WithWalDir(walDir),
			testserver.WithDataDir(dataDir),
			testserver.WithRaftPort(raftBasePort + i),
			testserver.WithGRPCPort(serviceBasePort + i),
			testserver.WithSnapshotThreshold(10),
			testserver.WithRaftCompactionMargin(1),
			testserver.WithDebug(os.Getenv("DEBUG") == "true"),
			testserver.WithRaftTickInterval(10 * time.Millisecond),
			testserver.WithRaftHeartbeatTick(1),
			testserver.WithRaftElectionTick(10),
			testserver.WithAutoPromoteThreshold(10),
			// TLS configuration — same cert for all nodes
			testserver.WithTLSCertFile(certs.ServerCertFile),
			testserver.WithTLSKeyFile(certs.ServerKeyFile),
			testserver.WithTLSCACertFile(certs.CACertFile),
		}
	}

	servers := make([]*tlsServiceWithClient, 0, countInstances)

	startNode := func(i int, extraInstruments ...testservice.Instrumentation) {
		walTmpDir := GinkgoT().TempDir()
		dataTmpDir := GinkgoT().TempDir()
		DeferCleanup(func() {
			Expect(os.RemoveAll(walTmpDir)).To(Succeed())
			Expect(os.RemoveAll(dataTmpDir)).To(Succeed())
		})

		instruments := commonInstruments(i, walTmpDir, dataTmpDir)
		instruments = append(instruments, extraInstruments...)

		server := testservice.New(cmdserver.NewRunCommand,
			testservice.WithInstruments(instruments...),
		)
		Expect(server.Start(ctx)).To(Succeed())

		// Create TLS-enabled gRPC client
		grpcClient, clusterClient, grpcConn, err := newTLSGRPCClient(serviceBasePort+i, certs.CACertFile)
		Expect(err).To(Succeed())
		DeferCleanup(func() {
			_ = grpcConn.Close()
		})

		servers = append(servers, &tlsServiceWithClient{
			service:       server,
			client:        grpcClient,
			clusterClient: clusterClient,
			grpcConn:      grpcConn,
			grpcPort:      serviceBasePort + i,
			nodeID:        uint32(i + 1),
		})
	}

	// Node 0: bootstrap a single-node cluster
	startNode(0, testserver.WithBootstrap())

	// Wait for node 0 to become leader
	Eventually(func(g Gomega) {
		state, err := servers[0].clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		g.Expect(err).To(Succeed())
		g.Expect(state.Leader).NotTo(BeZero())
	}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

	// Nodes 1..N-1: join the bootstrap node via TLS
	// The --join flag connects to the service port which is now TLS-enabled
	bootstrapServiceAddr := fmt.Sprintf("127.0.0.1:%d", serviceBasePort)
	for i := 1; i < countInstances; i++ {
		startNode(i, testserver.WithJoin(bootstrapServiceAddr))
	}

	// Wait for all nodes to be promoted to voters
	if countInstances > 1 {
		Eventually(func(g Gomega) {
			state, err := servers[0].clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			voterCount := 0
			for _, n := range state.Nodes {
				if n.Suffrage == "Voter" {
					voterCount++
				}
			}
			g.Expect(voterCount).To(Equal(countInstances))
		}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	}

	// Cleanup: stop all servers
	DeferCleanup(func() {
		for _, srv := range servers {
			_ = srv.grpcConn.Close()
		}
		for i, srv := range servers {
			By(fmt.Sprintf("Stopping TLS node %d", i+1), func() {
				stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				Expect(srv.service.Stop(stopCtx)).To(Succeed())
			})
		}
	})

	return ctx, servers, certs
}

var _ = Describe("TLS", Ordered, func() {
	var (
		ctx           context.Context
		client        servicepb.BucketServiceClient
		clusterClient clusterpb.ClusterServiceClient
		certs         *testserver.TestCerts
	)

	BeforeAll(func() {
		ctx, client, clusterClient, certs = setupTLSSingleNode(tlsTestHTTPPort, tlsTestGRPCPort, tlsTestRaftPort)
	})

	Context("with a TLS-enabled server", func() {
		It("should accept connections from a TLS client with the correct CA", func() {
			// The client was already created with the correct CA in setupTLSSingleNode.
			// Verify it can perform actual operations.
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("tls-test-ledger", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should serve data over TLS", func() {
			// Create a transaction to verify full round-trip
			resp, err := client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction("tls-test-ledger", []*commonpb.Posting{
						actions.NewPosting("world", "bank", big.NewInt(1000), "USD"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should return cluster state over TLS", func() {
			state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(Succeed())
			Expect(state.Leader).NotTo(BeZero())
			Expect(state.Nodes).NotTo(BeEmpty())
		})

		It("should reject connections from an insecure client", func() {
			// Create an insecure gRPC client (no TLS)
			conn, err := grpc.NewClient(
				fmt.Sprintf("localhost:%d", tlsTestGRPCPort),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
			)
			Expect(err).To(Succeed())
			defer func() { _ = conn.Close() }()

			insecureClient := clusterpb.NewClusterServiceClient(conn)
			_, err = insecureClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(HaveOccurred())
		})

		It("should reject connections from a TLS client without the correct CA", func() {
			// Create a TLS client that does NOT trust our test CA (uses empty pool)
			tlsCfg := &tls.Config{
				RootCAs:    x509.NewCertPool(), // Empty pool — won't trust our server cert
				MinVersion: tls.VersionTLS12,
			}

			conn, err := grpc.NewClient(
				fmt.Sprintf("localhost:%d", tlsTestGRPCPort),
				grpc.WithTransportCredentials(credentials.NewTLS(tlsCfg)),
			)
			Expect(err).To(Succeed())
			defer func() { _ = conn.Close() }()

			wrongCAClient := clusterpb.NewClusterServiceClient(conn)
			_, err = wrongCAClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(HaveOccurred())
		})

		It("should accept a second TLS client that loads the same CA", func() {
			// Simulate a fresh client reading the CA file
			client2, _, conn2, err := newTLSGRPCClient(tlsTestGRPCPort, certs.CACertFile)
			Expect(err).To(Succeed())
			defer func() { _ = conn2.Close() }()

			resp, err := client2.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("tls-test-ledger-2", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})
	})
})

var _ = Describe("TLS Multi-Node", Ordered, func() {
	var (
		ctx     context.Context
		servers []*tlsServiceWithClient
	)

	BeforeAll(func() {
		ctx, servers, _ = setupTLSMultiNodeCluster(3, tlsMultiRaftBasePort, tlsMultiServiceBasePort, tlsMultiHTTPBasePort)
	})

	Context("with a TLS-enabled multi-node cluster", func() {
		It("should elect a leader across TLS nodes", func() {
			state, err := servers[0].clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			Expect(err).To(Succeed())
			Expect(state.Leader).NotTo(BeZero())
			Expect(state.Nodes).To(HaveLen(3))
		})

		It("should replicate ledger creation across TLS nodes", func() {
			// Create a ledger via node 0
			resp, err := servers[0].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("tls-multi-ledger", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Verify the ledger is visible from all nodes
			for i, srv := range servers {
				Eventually(func(g Gomega) {
					ledgers, err := actions.ListLedgers(ctx, srv.client)
					g.Expect(err).To(Succeed())
					g.Expect(ledgers).To(HaveKey("tls-multi-ledger"))
				}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed(),
					"ledger should be visible on node %d", i+1)
			}
		})

		It("should replicate transactions across TLS nodes", func() {
			// Create a transaction via node 0.
			// Use Eventually because TLS handshake overhead can delay leader
			// stabilisation after voter promotions, causing transient "ledger not found".
			var resp *servicepb.ApplyResponse
			Eventually(func(g Gomega) {
				var err error
				resp, err = servers[0].client.Apply(ctx, &servicepb.ApplyRequest{
					Requests: []*servicepb.Request{
						actions.CreateTransactionAction("tls-multi-ledger", []*commonpb.Posting{
							actions.NewPosting("world", "bank", big.NewInt(5000), "USD"),
						}, nil, nil),
					},
				})
				g.Expect(err).To(Succeed())
				g.Expect(resp).NotTo(BeNil())
			}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())

			// Extract the transaction ID from the Apply response (IDs are 1-based).
			Expect(resp.Logs).NotTo(BeEmpty())
			applyLog := resp.Logs[0].Payload.GetApply()
			Expect(applyLog).NotTo(BeNil())
			created := applyLog.Log.Data.GetCreatedTransaction()
			Expect(created).NotTo(BeNil())
			txID := created.Transaction.Id

			// Verify the transaction is visible from all nodes
			for i, srv := range servers {
				Eventually(func(g Gomega) {
					txResp, err := srv.client.GetTransaction(ctx, &servicepb.GetTransactionRequest{
						Ledger:        "tls-multi-ledger",
						TransactionId: txID,
					})
					g.Expect(err).To(Succeed())
					g.Expect(txResp).NotTo(BeNil())
					g.Expect(txResp.Transaction.Postings).To(HaveLen(1))
				}).Within(15 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed(),
					"transaction should be visible on node %d", i+1)
			}
		})

		It("should allow operations through any TLS node", func() {
			// Create a ledger via node 1 (non-bootstrap)
			resp, err := servers[1].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateLedgerAction("tls-multi-ledger-node1", nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())

			// Create a transaction via node 2
			resp, err = servers[2].client.Apply(ctx, &servicepb.ApplyRequest{
				Requests: []*servicepb.Request{
					actions.CreateTransactionAction("tls-multi-ledger-node1", []*commonpb.Posting{
						actions.NewPosting("world", "alice", big.NewInt(100), "EUR"),
					}, nil, nil),
				},
			})
			Expect(err).To(Succeed())
			Expect(resp).NotTo(BeNil())
		})

		It("should return consistent cluster state from all TLS nodes", func() {
			for i, srv := range servers {
				state, err := srv.clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
				Expect(err).To(Succeed(), "node %d should return cluster state", i+1)
				Expect(state.Leader).NotTo(BeZero(), "node %d should report a leader", i+1)
				Expect(state.Nodes).To(HaveLen(3), "node %d should see all 3 nodes", i+1)
			}
		})
	})
})
