//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"math/big"
	"time"

	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

// serviceWithClient is a shared type used across e2e tests to hold a test service instance
// along with its gRPC client and directory paths.
type serviceWithClient struct {
	service  *testservice.Service
	client   servicepb.LedgerServiceClient
	grpcConn *grpc.ClientConn
	walDir   string
	dataDir  string
	grpcPort int
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
func newGRPCClient(grpcPort int) (servicepb.LedgerServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(retryUnaryInterceptor(10, 100*time.Millisecond)), // Retry up to 10 times with 100ms delay
	)
	if err != nil {
		return nil, nil, err
	}
	return servicepb.NewLedgerServiceClient(conn), conn, nil
}

// Helper functions for creating gRPC requests

// createLedgerAction creates an action for creating a new ledger
func createLedgerAction(name string, metadata map[string]string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name:     name,
				Metadata: metadata,
			},
		},
	}
}

// deleteLedgerAction creates an action for deleting a ledger
func deleteLedgerAction(ledgerID uint32) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{
				Id: ledgerID,
			},
		},
	}
}

// createTransactionAction creates an action for creating a transaction
func createTransactionAction(ledgerName string, postings []*commonpb.Posting, metadata map[string]string, accountMetadata map[string]*commonpb.Metadata) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyRequest_CreateTransaction{
					CreateTransaction: &servicepb.CreateTransactionPayload{
						Postings:        postings,
						Metadata:        metadata,
						AccountMetadata: accountMetadata,
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
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Account{
								Account: &commonpb.TargetAccount{Addr: address},
							},
						},
						Metadata: &commonpb.Metadata{Entries: metadata},
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
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
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
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyRequest_AddMetadata{
					AddMetadata: &commonpb.SaveMetadataCommand{
						Target: &commonpb.Target{
							Target: &commonpb.Target_Transaction{
								Transaction: &commonpb.TargetTransaction{Id: transactionID},
							},
						},
						Metadata: &commonpb.Metadata{Entries: metadata},
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
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
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
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyRequest_RevertTransaction{
					RevertTransaction: &servicepb.RevertTransactionPayload{
						TransactionId:   transactionID,
						Force:           force,
						AtEffectiveDate: atEffectiveDate,
						Metadata:        metadata,
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
func getAllLedgersInfo(ctx context.Context, client servicepb.LedgerServiceClient) (map[string]*commonpb.LedgerInfo, error) {
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
