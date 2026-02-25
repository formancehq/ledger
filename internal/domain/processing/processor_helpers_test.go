package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
)

// requestToOrder converts a servicepb.Request to a raftcmdpb.Order for test purposes.
func requestToOrder(req *servicepb.Request) *raftcmdpb.Order {
	order := &raftcmdpb.Order{}

	switch reqType := req.Type.(type) {
	case *servicepb.Request_CreateLedger:
		order.Type = &raftcmdpb.Order_CreateLedger{
			CreateLedger: &raftcmdpb.CreateLedgerOrder{
				Name:            reqType.CreateLedger.Name,
				ChartOfAccounts: reqType.CreateLedger.ChartOfAccounts,
				EnforcementMode: reqType.CreateLedger.EnforcementMode,
			},
		}
	case *servicepb.Request_DeleteLedger:
		order.Type = &raftcmdpb.Order_DeleteLedger{
			DeleteLedger: &raftcmdpb.DeleteLedgerOrder{
				Name: reqType.DeleteLedger.Name,
			},
		}
	case *servicepb.Request_Apply:
		applyOrder := &raftcmdpb.LedgerApplyOrder{
			Ledger: reqType.Apply.Ledger,
		}
		switch data := reqType.Apply.Data.(type) {
		case *servicepb.LedgerApplyRequest_CreateTransaction:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_CreateTransaction{
				CreateTransaction: &raftcmdpb.CreateTransactionOrder{
					Postings:  data.CreateTransaction.Postings,
					Script:    data.CreateTransaction.Script,
					Timestamp: data.CreateTransaction.Timestamp,
					Reference: data.CreateTransaction.Reference,
					Metadata:  data.CreateTransaction.Metadata,
					Force:     data.CreateTransaction.Force,
				},
			}
		case *servicepb.LedgerApplyRequest_AddMetadata:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_AddMetadata{
				AddMetadata: &raftcmdpb.SaveMetadataOrder{
					Target:   data.AddMetadata.Target,
					Metadata: data.AddMetadata.Metadata,
				},
			}
		case *servicepb.LedgerApplyRequest_DeleteMetadata:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_DeleteMetadata{
				DeleteMetadata: &raftcmdpb.DeleteMetadataOrder{
					Target: data.DeleteMetadata.Target,
					Key:    data.DeleteMetadata.Key,
				},
			}
		case *servicepb.LedgerApplyRequest_RevertTransaction:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_RevertTransaction{
				RevertTransaction: &raftcmdpb.RevertTransactionOrder{
					TransactionId:   data.RevertTransaction.TransactionId,
					Force:           data.RevertTransaction.Force,
					AtEffectiveDate: data.RevertTransaction.AtEffectiveDate,
					Metadata:        data.RevertTransaction.Metadata,
				},
			}
		case *servicepb.LedgerApplyRequest_SetChartOfAccounts:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_SetChartOfAccounts{
				SetChartOfAccounts: &raftcmdpb.SetChartOfAccountsOrder{
					ChartOfAccounts: data.SetChartOfAccounts.ChartOfAccounts,
				},
			}
		case *servicepb.LedgerApplyRequest_SetChartEnforcementMode:
			applyOrder.Data = &raftcmdpb.LedgerApplyOrder_SetChartEnforcementMode{
				SetChartEnforcementMode: &raftcmdpb.SetChartEnforcementModeOrder{
					EnforcementMode: data.SetChartEnforcementMode.EnforcementMode,
				},
			}
		}
		order.Type = &raftcmdpb.Order_Apply{
			Apply: applyOrder,
		}
	}

	return order
}
