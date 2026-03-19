package testutil

import (
	"context"
	"fmt"
	"os"
	"time"

	. "github.com/onsi/ginkgo/v2" //nolint:staticcheck // dot import is idiomatic for Ginkgo test helpers
	. "github.com/onsi/gomega"    //nolint:staticcheck // dot import is idiomatic for Gomega test helpers
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/formancehq/go-libs/v4/logging"
	"github.com/formancehq/go-libs/v4/testing/testservice"

	cmdserver "github.com/formancehq/ledger-v3-poc/cmd/server"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
)

// Common port constants shared by all e2e tests.
// All tests run sequentially, so they can safely reuse the same ports.
// Using high ports (15xxx) to avoid conflicts with host services.
const (
	// Multi-node test ports (up to 4 nodes: base+0, base+1, base+2, base+3).
	TestRaftBasePort    = 15000
	TestServiceBasePort = 15100
	TestHTTPBasePort    = 15200
	TestGatewayBasePort = 15300

	// Single-node test ports (raft port is derived as TestSingleGRPCPort - 1000).
	TestSingleHTTPPort = 15200
	TestSingleGRPCPort = 15100
)

var (
	Debug = os.Getenv("DEBUG") == "true"
)

// ServiceWithClient holds a test service instance along with its gRPC clients and directory paths.
type ServiceWithClient struct {
	Service       *testservice.Service
	Client        servicepb.BucketServiceClient
	ClusterClient clusterpb.ClusterServiceClient
	GRPCConn      *grpc.ClientConn
	WalDir        string
	DataDir       string
	GRPCPort      int
	NodeID        uint32
}

// GRPCRetryPolicy defines the retry policy for gRPC clients when no leader is available.
var GRPCRetryPolicy = `{
	"methodConfig": [{
		"name": [{}],
		"retryPolicy": {
			"MaxAttempts": 50,
			"InitialBackoff": "0.2s",
			"MaxBackoff": "0.2s",
			"BackoffMultiplier": 1.0,
			"RetryableStatusCodes": ["UNAVAILABLE"]
		}
	}]
}`

// NewGRPCClient creates a new gRPC client connection for a given port with automatic retry on Unavailable errors.
func NewGRPCClient(grpcPort int) (servicepb.BucketServiceClient, clusterpb.ClusterServiceClient, *grpc.ClientConn, error) {
	return NewGRPCClientWithRetry(grpcPort, true)
}

// NewGRPCClientWithRetry creates a new gRPC client with optional retry policy.
func NewGRPCClientWithRetry(grpcPort int, withRetry bool) (servicepb.BucketServiceClient, clusterpb.ClusterServiceClient, *grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if withRetry {
		opts = append(opts, grpc.WithDefaultServiceConfig(GRPCRetryPolicy))
	}

	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", grpcPort),
		opts...,
	)
	if err != nil {
		return nil, nil, nil, err
	}

	return servicepb.NewBucketServiceClient(conn), clusterpb.NewClusterServiceClient(conn), conn, nil
}

// MultiNodeOptions holds configuration options for SetupMultiNodeCluster.
type MultiNodeOptions struct {
	WithGateway      bool
	RaftTickInterval time.Duration
}

// MultiNodeOption is a functional option for SetupMultiNodeCluster.
type MultiNodeOption func(*MultiNodeOptions)

// WithGateway enables the test gateway for intercepting Raft traffic.
func WithGateway() MultiNodeOption {
	return func(o *MultiNodeOptions) {
		o.WithGateway = true
	}
}

// WithTickInterval sets the Raft tick interval (default: 10ms).
func WithTickInterval(d time.Duration) MultiNodeOption {
	return func(o *MultiNodeOptions) {
		o.RaftTickInterval = d
	}
}

// SetupMultiNodeCluster creates a multi-node Raft cluster for e2e tests.
// It returns the context, the list of services with clients, the gateway (if enabled), and a pointer to the current leader ID.
// Cleanup is handled automatically via DeferCleanup.
func SetupMultiNodeCluster(
	countInstances int,
	raftBasePort, serviceBasePort, httpBasePort, gatewayBasePort int,
	opts ...MultiNodeOption,
) (context.Context, []*ServiceWithClient, *testserver.Gateway, *uint64) {
	options := MultiNodeOptions{
		RaftTickInterval: 10 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&options)
	}

	ctx := logging.TestingContext()

	var gw *testserver.Gateway
	if options.WithGateway {
		gatewayPorts := make([]int, countInstances)
		nodeRaftAddresses := make([]string, countInstances)
		for i := range countInstances {
			gatewayPorts[i] = gatewayBasePort + i
			nodeRaftAddresses[i] = fmt.Sprintf("127.0.0.1:%d", raftBasePort+i)
		}

		var err error
		gw, err = testserver.NewGateway(logging.FromContext(ctx), gatewayPorts, nodeRaftAddresses)
		Expect(err).To(Succeed())

		Expect(gw.Start(ctx)).To(Succeed())
		DeferCleanup(func() {
			stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			Expect(gw.Stop(stopCtx)).To(Succeed())
		})
	}

	// Common instruments shared by all nodes
	commonInstruments := func(i int, walDir, dataDir string) []testservice.Instrumentation {
		return []testservice.Instrumentation{
			testservice.DebugInstrumentation(Debug),
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
			testserver.WithRaftTickInterval(options.RaftTickInterval),
			testserver.WithRaftHeartbeatTick(1),
			testserver.WithRaftElectionTick(10),
			testserver.WithAutoPromoteThreshold(10),
		}
	}

	servers := make([]*ServiceWithClient, 0, countInstances)

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

		grpcClient, clusterClient, grpcConn, err := NewGRPCClient(serviceBasePort + i)
		Expect(err).To(Succeed())
		DeferCleanup(func() {
			_ = grpcConn.Close()
		})

		servers = append(servers, &ServiceWithClient{
			Service:       server,
			Client:        grpcClient,
			ClusterClient: clusterClient,
			GRPCConn:      grpcConn,
			WalDir:        walTmpDir,
			DataDir:       dataTmpDir,
			GRPCPort:      serviceBasePort + i,
			NodeID:        uint32(i + 1),
		})
	}

	// Node 0: bootstrap a single-node cluster
	startNode(0, testserver.WithBootstrap())

	// Wait for node 0 to become leader before joining other nodes
	var leaderID uint64
	Eventually(func(g Gomega) {
		state, err := servers[0].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		g.Expect(err).To(Succeed())
		g.Expect(state.GetLeader()).NotTo(BeZero())
		leaderID = uint64(state.GetLeader())
	}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

	// Nodes 1..N-1: join the bootstrap node
	bootstrapServiceAddr := fmt.Sprintf("127.0.0.1:%d", serviceBasePort)
	for i := 1; i < countInstances; i++ {
		startNode(i, testserver.WithJoin(bootstrapServiceAddr))
	}

	// Wait for all nodes to be promoted to voters
	if countInstances > 1 {
		Eventually(func(g Gomega) {
			state, err := servers[0].ClusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
			g.Expect(err).To(Succeed())
			voterCount := 0
			for _, n := range state.GetNodes() {
				if n.GetSuffrage() == "Voter" {
					voterCount++
				}
			}
			g.Expect(voterCount).To(Equal(countInstances))
		}).Within(30 * time.Second).ProbeEvery(200 * time.Millisecond).Should(Succeed())
	}

	return ctx, servers, gw, &leaderID
}

// StopNode gracefully stops a node by first closing its gRPC connection
// to prevent in-flight requests from racing with pebble shutdown.
func StopNode(ctx context.Context, srv *ServiceWithClient) {
	_ = srv.GRPCConn.Close()
	Expect(srv.Service.Stop(ctx)).To(Succeed())
}

// RestartNode starts a previously stopped node and recreates its gRPC client connection.
func RestartNode(ctx context.Context, srv *ServiceWithClient) {
	Expect(srv.Service.Start(ctx)).To(Succeed())
	client, clusterClient, conn, err := NewGRPCClient(srv.GRPCPort)
	Expect(err).To(Succeed())
	srv.Client = client
	srv.ClusterClient = clusterClient
	srv.GRPCConn = conn
}

// StopServers stops all servers in the list. Used in AfterEach/AfterAll blocks.
func StopServers(ctx context.Context, servers []*ServiceWithClient) {
	for _, server := range servers {
		_ = server.GRPCConn.Close()
	}
	for i, server := range servers {
		By(fmt.Sprintf("Stopping node %d", i+1), func() {
			stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			DeferCleanup(cancel)
			Expect(server.Service.Stop(stopCtx)).To(Succeed())
		})
	}
}

// SetupSingleNode creates a single-node cluster for tests that don't need Raft consensus.
// Returns the context, client, and cluster client.
// Cleanup is handled automatically via DeferCleanup.
func SetupSingleNode(httpPort, grpcPort int, extraInstruments ...testservice.Instrumentation) (context.Context, servicepb.BucketServiceClient, clusterpb.ClusterServiceClient) {
	ctx := logging.TestingContext()

	walTmpDir := GinkgoT().TempDir()
	dataTmpDir := GinkgoT().TempDir()
	DeferCleanup(func() {
		Expect(os.RemoveAll(walTmpDir)).To(Succeed())
		Expect(os.RemoveAll(dataTmpDir)).To(Succeed())
	})

	// Derive Raft port from gRPC port (e.g., 8100 -> 7100)
	raftPort := grpcPort - 1000

	instruments := []testservice.Instrumentation{
		testservice.DebugInstrumentation(Debug),
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
		testserver.WithRaftTickInterval(10 * time.Millisecond),
		testserver.WithRaftHeartbeatTick(1),
		testserver.WithRaftElectionTick(10),
		testserver.WithBootstrap(),
	}
	instruments = append(instruments, extraInstruments...)

	server := testservice.New(cmdserver.NewRunCommand,
		testservice.WithInstruments(instruments...),
	)
	Expect(server.Start(ctx)).To(Succeed())

	// Cleanup server on test end
	DeferCleanup(func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		Expect(server.Stop(stopCtx)).To(Succeed())
	})

	// Create gRPC client
	grpcClient, clusterClient, grpcConn, err := NewGRPCClient(grpcPort)
	Expect(err).To(Succeed())
	DeferCleanup(func() {
		_ = grpcConn.Close()
	})

	// Wait for leader election (single node elects itself)
	Eventually(func(g Gomega) bool {
		state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		g.Expect(err).To(Succeed())

		return state.GetLeader() != 0
	}).Within(5 * time.Second).To(BeTrue())

	return ctx, grpcClient, clusterClient
}
