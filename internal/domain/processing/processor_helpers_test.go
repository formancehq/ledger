package processing

import (
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// requestToOrder converts a servicepb.Request to a raftcmdpb.Order for test purposes.
func requestToOrder(req *servicepb.Request) *raftcmdpb.Order {
	order := &raftcmdpb.Order{}

	switch reqType := req.GetType().(type) {
	case *servicepb.Request_CreateLedger:
		order.Type = &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name: reqType.CreateLedger.GetName(),
			},
		}
	case *servicepb.Request_DeleteLedger:
		order.Type = &raftcmdpb.Order_DeleteLedger{
			DeleteLedger: &raftcmdpb.DeleteLedgerOrder{
				Name: reqType.DeleteLedger.GetName(),
			},
		}
	case *servicepb.Request_Apply:
		applyOrder := &raftcmdpb.LedgerApplyOrder{
			Ledger: reqType.Apply.GetLedger(),
		}
		switch data := reqType.Apply.GetAction().GetData().(type) {
		case *servicepb.LedgerAction_CreateTransaction:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_CreateTransaction{
				CreateTransaction: &raftcmdpb.CreateTransactionOrder{
					Postings:        data.CreateTransaction.GetPostings(),
					Script:          data.CreateTransaction.GetScript(),
					Timestamp:       data.CreateTransaction.GetTimestamp(),
					Reference:       data.CreateTransaction.GetReference(),
					Metadata:        data.CreateTransaction.GetMetadata(),
					AccountMetadata: data.CreateTransaction.GetAccountMetadata(),
					Force:           data.CreateTransaction.GetForce(),
				},
			}
		case *servicepb.LedgerAction_AddMetadata:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_AddMetadata{
				AddMetadata: &raftcmdpb.SaveMetadataOrder{
					Target:   data.AddMetadata.GetTarget(),
					Metadata: data.AddMetadata.GetMetadata(),
				},
			}
		case *servicepb.LedgerAction_DeleteMetadata:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
				DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
					Target: data.DeleteMetadata.GetTarget(),
					Key:    data.DeleteMetadata.GetKey(),
				},
			}
		case *servicepb.LedgerAction_RevertTransaction:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_RevertTransaction{
				RevertTransaction: &raftcmdpb.RevertTransactionOrder{
					TransactionId:   data.RevertTransaction.GetTransactionId(),
					Force:           data.RevertTransaction.GetForce(),
					AtEffectiveDate: data.RevertTransaction.GetAtEffectiveDate(),
					Metadata:        data.RevertTransaction.GetMetadata(),
				},
			}
		}

		order.Type = &raftcmdpb.Order_Apply{
			Apply: applyOrder,
		}
	}

	return order
}

// constantScopeFactory yields the same Scope for every NewScope call,
// regardless of coverage_bits / production_bits. Used by mock-based
// tests where the scope is a gomock — the bits are not exercised.
type constantScopeFactory struct{ scope Scope }

func (f constantScopeFactory) NewScope(_ []byte) (Scope, error) { return f.scope, nil }
func (f constantScopeFactory) NewProposalScope() (Scope, error) { return f.scope, nil }

// mockFactory returns a ScopeFactory that always yields the given scope.
func mockFactory(s Scope) ScopeFactory {
	return constantScopeFactory{scope: s}
}
