//go:build e2e

package e2e

import (
	"fmt"
	"math/big"

	"github.com/formancehq/go-libs/v3/testing/testservice"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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

// newGRPCClient creates a new gRPC client connection for a given port.
func newGRPCClient(grpcPort int) (servicepb.LedgerServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.NewClient(
		fmt.Sprintf("localhost:%d", grpcPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		return nil, nil, err
	}
	return servicepb.NewLedgerServiceClient(conn), conn, nil
}

// Helper functions for creating gRPC requests

// createLedgerAction creates an action for creating a new ledger
func createLedgerAction(name string, metadata map[string]string) *servicepb.Action {
	return &servicepb.Action{
		Type: &servicepb.Action_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name:     name,
				Metadata: metadata,
			},
		},
	}
}

// deleteLedgerAction creates an action for deleting a ledger
func deleteLedgerAction(ledgerID uint32) *servicepb.Action {
	return &servicepb.Action{
		Type: &servicepb.Action_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{
				Id: ledgerID,
			},
		},
	}
}

// createTransactionAction creates an action for creating a transaction
func createTransactionAction(ledgerName string, postings []*commonpb.Posting, metadata map[string]string, accountMetadata map[string]*commonpb.Metadata) *servicepb.Action {
	return &servicepb.Action{
		Type: &servicepb.Action_Apply{
			Apply: &servicepb.LedgerApplyAction{
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyAction_CreateTransaction{
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
func saveAccountMetadataAction(ledgerName, address string, metadata map[string]string) *servicepb.Action {
	return &servicepb.Action{
		Type: &servicepb.Action_Apply{
			Apply: &servicepb.LedgerApplyAction{
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyAction_AddMetadata{
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
func deleteAccountMetadataAction(ledgerName, address, key string) *servicepb.Action {
	return &servicepb.Action{
		Type: &servicepb.Action_Apply{
			Apply: &servicepb.LedgerApplyAction{
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyAction_DeleteMetadata{
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
func saveTransactionMetadataAction(ledgerName string, transactionID uint64, metadata map[string]string) *servicepb.Action {
	return &servicepb.Action{
		Type: &servicepb.Action_Apply{
			Apply: &servicepb.LedgerApplyAction{
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyAction_AddMetadata{
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
func deleteTransactionMetadataAction(ledgerName string, transactionID uint64, key string) *servicepb.Action {
	return &servicepb.Action{
		Type: &servicepb.Action_Apply{
			Apply: &servicepb.LedgerApplyAction{
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyAction_DeleteMetadata{
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
func revertTransactionAction(ledgerName string, transactionID uint64, force, atEffectiveDate bool, metadata map[string]string) *servicepb.Action {
	return &servicepb.Action{
		Type: &servicepb.Action_Apply{
			Apply: &servicepb.LedgerApplyAction{
				Ledger: &servicepb.LedgerNameOrId{
					Type: &servicepb.LedgerNameOrId_Name{Name: ledgerName},
				},
				Data: &servicepb.LedgerApplyAction_RevertTransaction{
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
