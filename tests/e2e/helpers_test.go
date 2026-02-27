//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"os"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/testing/testservice"
	cmdserver "github.com/formancehq/ledger-v3-poc/cmd/server"
	"github.com/formancehq/ledger-v3-poc/internal/proto/clusterpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/pkg/testserver"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// Common port constants shared by all e2e tests.
// All tests run sequentially, so they can safely reuse the same ports.
// Using high ports (15xxx) to avoid conflicts with host services.
const (
	// Multi-node test ports (up to 4 nodes: base+0, base+1, base+2, base+3)
	testRaftBasePort    = 15000
	testServiceBasePort = 15100
	testHTTPBasePort    = 15200
	testGatewayBasePort = 15300

	// Single-node test ports (raft port is derived as testSingleGRPCPort - 1000)
	testSingleHTTPPort = 15200
	testSingleGRPCPort = 15100
)

// extractGRPCErrorInfo extracts the ErrorInfo detail from a gRPC error.
func extractGRPCErrorInfo(err error) *errdetails.ErrorInfo {
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info
		}
	}
	return nil
}

// serviceWithClient is a shared type used across e2e tests to hold a test service instance
// along with its gRPC clients and directory paths.
type serviceWithClient struct {
	service       *testservice.Service
	client        servicepb.BucketServiceClient
	clusterClient clusterpb.ClusterServiceClient
	grpcConn      *grpc.ClientConn
	walDir        string
	dataDir       string
	grpcPort      int
	nodeID        uint32
}

// grpcRetryPolicy defines the retry policy for gRPC clients when no leader is available
var grpcRetryPolicy = `{
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

// newGRPCClient creates a new gRPC client connection for a given port with automatic retry on Unavailable errors.
func newGRPCClient(grpcPort int) (servicepb.BucketServiceClient, clusterpb.ClusterServiceClient, *grpc.ClientConn, error) {
	return newGRPCClientWithRetry(grpcPort, true)
}

// newGRPCClientWithRetry creates a new gRPC client with optional retry policy
func newGRPCClientWithRetry(grpcPort int, withRetry bool) (servicepb.BucketServiceClient, clusterpb.ClusterServiceClient, *grpc.ClientConn, error) {
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}

	if withRetry {
		opts = append(opts, grpc.WithDefaultServiceConfig(grpcRetryPolicy)) // Retry on UNAVAILABLE (no leader) up to 50 times with 200ms delay (10s max)
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

// Helper functions for creating gRPC requests

// createLedgerAction creates an action for creating a new ledger
func createLedgerAction(name string, _ map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: name,
			},
		},
	}
}

// deleteLedgerAction creates an action for deleting a ledger
func deleteLedgerAction(ledgerName string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{
				Name: ledgerName,
			},
		},
	}
}

// createTransactionAction creates an action for creating a transaction
func createTransactionAction(ledgerName string, postings []*commonpb.Posting, metadata map[string]string, accountMetadata map[string]*commonpb.MetadataSet) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings:        postings,
						Metadata:        commonpb.MetadataSetFromMap(metadata),
						AccountMetadata: accountMetadata,
					},
				},
			},
		},
	}
}

// createForceTransactionAction creates an action for creating a transaction with force=true (bypasses balance checks)
func createForceTransactionAction(ledgerName string, postings []*commonpb.Posting, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings: postings,
						Metadata: commonpb.MetadataSetFromMap(metadata),
						Force:    true,
					},
				},
			},
		},
	}
}

// createForceScriptTransactionAction creates an action for creating a transaction using Numscript with force=true
func createForceScriptTransactionAction(ledgerName string, script string, vars map[string]string, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: script,
							Vars:  vars,
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
						Force:    true,
					},
				},
			},
		},
	}
}

// createScriptTransactionAction creates an action for creating a transaction using Numscript
func createScriptTransactionAction(ledgerName string, script string, vars map[string]string, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Script: &commonpb.Script{
							Plain: script,
							Vars:  vars,
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

// saveAccountMetadataAction creates an action for saving account metadata
func saveAccountMetadataAction(ledgerName, address string, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

// deleteAccountMetadataAction creates an action for deleting account metadata
func deleteAccountMetadataAction(ledgerName, address, key string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Key: key,
					},
				},
			},
		},
	}
}

// saveTransactionMetadataAction creates an action for saving transaction metadata
func saveTransactionMetadataAction(ledgerName string, transactionID uint64, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{Id: transactionID},
							},
						},
						Metadata: commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

// deleteTransactionMetadataAction creates an action for deleting transaction metadata
func deleteTransactionMetadataAction(ledgerName string, transactionID uint64, key string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_DeleteMetadata{
					DeleteMetadata: &commonpb.DeleteMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{Id: transactionID},
							},
						},
						Key: key,
					},
				},
			},
		},
	}
}

// revertTransactionAction creates an action for reverting a transaction
func revertTransactionAction(ledgerName string, transactionID uint64, force, atEffectiveDate bool, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_RevertTransaction{
					RevertTransaction: &servicepb.RevertTransactionPayload{
						TransactionId:   transactionID,
						Force:           force,
						AtEffectiveDate: atEffectiveDate,
						Metadata:        commonpb.MetadataSetFromMap(metadata),
					},
				},
			},
		},
	}
}

// withTimestamp sets the timestamp on a create transaction request
func withTimestamp(req *servicepb.Request, t time.Time) *servicepb.Request {
	switch reqType := req.Type.(type) {
	case *servicepb.Request_Apply:
		switch d := reqType.Apply.Data.(type) {
		case *servicepb.LedgerApplyRequest_CreateTransaction:
			d.CreateTransaction.Timestamp = &commonpb.Timestamp{Data: uint64(t.UnixMicro())}
		}
	}
	return req
}

// withExpandVolumes sets the ExpandVolumes flag on a create or revert transaction request
func withExpandVolumes(req *servicepb.Request) *servicepb.Request {
	switch reqType := req.Type.(type) {
	case *servicepb.Request_Apply:
		switch d := reqType.Apply.Data.(type) {
		case *servicepb.LedgerApplyRequest_CreateTransaction:
			d.CreateTransaction.ExpandVolumes = true
		case *servicepb.LedgerApplyRequest_RevertTransaction:
			d.RevertTransaction.ExpandVolumes = true
		}
	}
	return req
}

// newPosting creates a new posting protobuf message
func newPosting(source, destination string, amount *big.Int, asset string) *commonpb.Posting {
	return commonpb.NewPosting(source, destination, asset, amount)
}

// listLedgers collects all ledgers from the streaming RPC into a map
func listLedgers(ctx context.Context, client servicepb.BucketServiceClient) (map[string]*commonpb.LedgerInfo, error) {
	stream, err := client.ListLedgers(ctx, &servicepb.ListLedgersRequest{})
	if err != nil {
		return nil, err
	}

	ledgers := make(map[string]*commonpb.LedgerInfo)
	for {
		ledger, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		ledgers[ledger.Name] = ledger
	}

	return ledgers, nil
}

// multiNodeOptions holds configuration options for setupMultiNodeCluster.
type multiNodeOptions struct {
	withGateway      bool
	raftTickInterval time.Duration
}

// MultiNodeOption is a functional option for setupMultiNodeCluster.
type MultiNodeOption func(*multiNodeOptions)

// WithGateway enables the test gateway for intercepting Raft traffic.
func WithGateway() MultiNodeOption {
	return func(o *multiNodeOptions) {
		o.withGateway = true
	}
}

// WithTickInterval sets the Raft tick interval (default: 10ms).
func WithTickInterval(d time.Duration) MultiNodeOption {
	return func(o *multiNodeOptions) {
		o.raftTickInterval = d
	}
}

// setupMultiNodeCluster creates a multi-node Raft cluster for e2e tests.
// It returns the context, the list of services with clients, the gateway (if enabled), and a pointer to the current leader ID.
// Cleanup is handled automatically via DeferCleanup.
func setupMultiNodeCluster(
	countInstances int,
	raftBasePort, serviceBasePort, httpBasePort, gatewayBasePort int,
	opts ...MultiNodeOption,
) (context.Context, []*serviceWithClient, *testserver.Gateway, *uint64) {
	options := multiNodeOptions{
		raftTickInterval: 10 * time.Millisecond,
	}
	for _, opt := range opts {
		opt(&options)
	}

	ctx := logging.TestingContext()

	var gw *testserver.Gateway
	if options.withGateway {
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
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
			testserver.WithNodeID(i + 1),
			testserver.WithHTTPPort(httpBasePort + i),
			testserver.WithWalDir(walDir),
			testserver.WithDataDir(dataDir),
			testserver.WithRaftPort(raftBasePort + i),
			testserver.WithGRPCPort(serviceBasePort + i),
			testserver.WithSnapshotThreshold(10),
			testserver.WithRaftCompactionMargin(1),
			testserver.WithDebug(os.Getenv("DEBUG") == "true"),
			testserver.WithRaftTickInterval(options.raftTickInterval),
			testserver.WithRaftHeartbeatTick(1),
			testserver.WithRaftElectionTick(10),
			testserver.WithAutoPromoteThreshold(10),
		}
	}

	servers := make([]*serviceWithClient, 0, countInstances)

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

		grpcClient, clusterClient, grpcConn, err := newGRPCClient(serviceBasePort + i)
		Expect(err).To(Succeed())
		DeferCleanup(func() {
			_ = grpcConn.Close()
		})

		servers = append(servers, &serviceWithClient{
			service:       server,
			client:        grpcClient,
			clusterClient: clusterClient,
			grpcConn:      grpcConn,
			walDir:        walTmpDir,
			dataDir:       dataTmpDir,
			grpcPort:      serviceBasePort + i,
			nodeID:        uint32(i + 1),
		})
	}

	// Node 0: bootstrap a single-node cluster
	startNode(0, testserver.WithBootstrap())

	// Wait for node 0 to become leader before joining other nodes
	var leaderID uint64
	Eventually(func(g Gomega) {
		state, err := servers[0].clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		g.Expect(err).To(Succeed())
		g.Expect(state.Leader).NotTo(BeZero())
		leaderID = uint64(state.Leader)
	}).Within(10 * time.Second).ProbeEvery(100 * time.Millisecond).Should(Succeed())

	// Nodes 1..N-1: join the bootstrap node
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

	return ctx, servers, gw, &leaderID
}

// stopNode gracefully stops a node by first closing its gRPC connection
// to prevent in-flight requests from racing with pebble shutdown.
func stopNode(ctx context.Context, srv *serviceWithClient) {
	_ = srv.grpcConn.Close()
	Expect(srv.service.Stop(ctx)).To(Succeed())
}

// restartNode starts a previously stopped node and recreates its gRPC client connection.
func restartNode(ctx context.Context, srv *serviceWithClient) {
	Expect(srv.service.Start(ctx)).To(Succeed())
	client, clusterClient, conn, err := newGRPCClient(srv.grpcPort)
	Expect(err).To(Succeed())
	srv.client = client
	srv.clusterClient = clusterClient
	srv.grpcConn = conn
}

// stopServers stops all servers in the list. Used in AfterEach/AfterAll blocks.
func stopServers(ctx context.Context, servers []*serviceWithClient) {
	for _, server := range servers {
		_ = server.grpcConn.Close()
	}
	for i, server := range servers {
		By(fmt.Sprintf("Stopping node %d", i+1), func() {
			stopCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			DeferCleanup(cancel)
			Expect(server.service.Stop(stopCtx)).To(Succeed())
		})
	}
}

// setupSingleNode creates a single-node cluster for tests that don't need Raft consensus.
// This simplifies test setup and speeds up test execution.
// Returns the context, client, and cleanup function.
// Optional extraInstruments can be provided to customize the server (e.g., WithReceiptSigningKey).
func setupSingleNode(httpPort, grpcPort int, extraInstruments ...testservice.Instrumentation) (context.Context, servicepb.BucketServiceClient, clusterpb.ClusterServiceClient) {
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
		testservice.DebugInstrumentation(debug),
		testservice.OutputInstrumentation(GinkgoWriter),
		testserver.WithNodeID(1),
		testserver.WithHTTPPort(httpPort),
		testserver.WithWalDir(walTmpDir),
		testserver.WithDataDir(dataTmpDir),
		testserver.WithRaftPort(raftPort), // Internal Raft transport
		testserver.WithGRPCPort(grpcPort), // External service API
		testserver.WithSnapshotThreshold(10),
		testserver.WithDebug(os.Getenv("DEBUG") == "true"),
		testserver.WithRaftTickInterval(10*time.Millisecond),
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
	grpcClient, clusterClient, grpcConn, err := newGRPCClient(grpcPort)
	Expect(err).To(Succeed())
	DeferCleanup(func() {
		_ = grpcConn.Close()
	})

	// Wait for leader election (single node elects itself)
	Eventually(func(g Gomega) bool {
		state, err := clusterClient.GetClusterState(ctx, &clusterpb.GetClusterStateRequest{})
		g.Expect(err).To(Succeed())
		return state.Leader != 0
	}).Within(5 * time.Second).To(BeTrue())

	return ctx, grpcClient, clusterClient
}

// setMetadataFieldTypeAction creates a request to declare a metadata field type.
func setMetadataFieldTypeAction(ledger string, targetType commonpb.TargetType, key string, metadataType commonpb.MetadataType) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: targetType,
				Key:        key,
				Type:       metadataType,
			},
		},
	}
}

// removeMetadataFieldTypeAction creates a request to remove a metadata field type declaration.
func removeMetadataFieldTypeAction(ledger string, targetType commonpb.TargetType, key string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: targetType,
				Key:        key,
			},
		},
	}
}

// createLedgerWithSchemaAction creates a ledger with an initial metadata schema.
func createLedgerWithSchemaAction(name string, _ map[string]string, schema []*commonpb.SetMetadataFieldTypeCommand) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name:          name,
				InitialSchema: schema,
			},
		},
	}
}

// saveTypedAccountMetadataAction creates a request with a typed MetadataSet (not map[string]string).
func saveTypedAccountMetadataAction(ledgerName, address string, metadata *commonpb.MetadataSet) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Metadata: metadata,
					},
				},
			},
		},
	}
}

// saveTypedTransactionMetadataAction creates a request with a typed MetadataSet (not map[string]string).
func saveTypedTransactionMetadataAction(ledgerName string, txID uint64, metadata *commonpb.MetadataSet) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledgerName,
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{Id: txID},
							},
						},
						Metadata: metadata,
					},
				},
			},
		},
	}
}

// findMetadataValue looks up a key in a MetadataSet and returns the *MetadataValue (nil if not found).
func findMetadataValue(ms *commonpb.MetadataSet, key string) *commonpb.MetadataValue {
	if ms == nil {
		return nil
	}
	for _, md := range ms.Metadata {
		if md.Key == key {
			return md.Value
		}
	}
	return nil
}
