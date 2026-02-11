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
func createLedgerAction(name string, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name:     name,
				Metadata: commonpb.MetadataSetFromMap(metadata),
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

// newPosting creates a new posting protobuf message
func newPosting(source, destination string, amount *big.Int, asset string) *commonpb.Posting {
	return &commonpb.Posting{
		Source:      source,
		Destination: destination,
		Amount:      commonpb.NewBigInt(amount),
		Asset:       asset,
	}
}

// getAllLedgersInfo collects all ledgers from the streaming RPC into a map
func getAllLedgersInfo(ctx context.Context, client servicepb.BucketServiceClient) (map[string]*commonpb.LedgerInfo, error) {
	stream, err := client.GetAllLedgersInfo(ctx, &servicepb.GetAllLedgersRequest{})
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

// setupSingleNode creates a single-node cluster for tests that don't need Raft consensus.
// This simplifies test setup and speeds up test execution.
// Returns the context, client, and cleanup function.
func setupSingleNode(httpPort, grpcPort int) (context.Context, servicepb.BucketServiceClient, clusterpb.ClusterServiceClient) {
	ctx := logging.TestingContext()

	walTmpDir := GinkgoT().TempDir()
	dataTmpDir := GinkgoT().TempDir()
	DeferCleanup(func() {
		Expect(os.RemoveAll(walTmpDir)).To(Succeed())
		Expect(os.RemoveAll(dataTmpDir)).To(Succeed())
	})

	// Derive Raft port from gRPC port (e.g., 8100 -> 7100)
	raftPort := grpcPort - 1000

	server := testservice.New(cmdserver.NewRunCommand,
		testservice.WithInstruments(
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
			// No peers needed for single-node cluster
		),
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
