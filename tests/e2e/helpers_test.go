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
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

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
}

// retryUnaryInterceptor retries the request on Unavailable errors (e.g., no leader)
func retryUnaryInterceptor(maxRetries int, retryDelay time.Duration) grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		var lastErr error
		for attempt := 0; attempt <= maxRetries; attempt++ {
			lastErr = invoker(ctx, method, req, reply, cc, opts...)
			if lastErr == nil {
				return nil
			}

			// Check if the error is retryable (Unavailable = no leader)
			st, ok := status.FromError(lastErr)
			if !ok || st.Code() != codes.Unavailable {
				// Not a retryable error
				return lastErr
			}

			// Wait before retrying (unless it's the last attempt)
			if attempt < maxRetries {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(retryDelay):
				}
			}
		}
		return lastErr
	}
}

// newGRPCClient creates a new gRPC client connection for a given port with automatic retry on Unavailable errors.
func newGRPCClient(grpcPort int) (servicepb.BucketServiceClient, clusterpb.ClusterServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(retryUnaryInterceptor(10, 100*time.Millisecond)), // Retry up to 10 times with 100ms delay
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

	server := testservice.New(cmdserver.NewRunCommand,
		testservice.WithInstruments(
			testservice.DebugInstrumentation(debug),
			testservice.OutputInstrumentation(GinkgoWriter),
			testserver.WithNodeID(1),
			testserver.WithHTTPPort(httpPort),
			testserver.WithWalDir(walTmpDir),
			testserver.WithDataDir(dataTmpDir),
			testserver.WithGRPCPort(grpcPort),
			testserver.WithSnapshotThreshold(10),
			testserver.WithDebug(os.Getenv("DEBUG") == "true"),
			testserver.WithRaftTickInterval(10*time.Millisecond),
			testserver.WithRaftHeartbeatTick(10),
			testserver.WithRaftElectionTick(100),
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
	}).Within(15 * time.Second).To(BeTrue())

	return ctx, grpcClient, clusterClient
}
