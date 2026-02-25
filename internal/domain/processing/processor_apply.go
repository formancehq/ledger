package processing

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processApply(apply *raftcmdpb.LedgerApplyOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	boundaries, ok := s.GetBoundaries(apply.Ledger)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: apply.Ledger}
	}

	// Block data-modifying writes on mirror-mode ledgers.
	// Schema operations (set/remove metadata field type and their conversion
	// lifecycle) are allowed because they only affect local configuration,
	// not replicated data.
	if ledgerInfo, infoOk := s.GetLedger(apply.Ledger); infoOk && ledgerInfo.Mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		if !isMirrorSafeApply(apply) {
			return nil, &domain.ErrLedgerInMirrorMode{Name: apply.Ledger}
		}
	}

	var (
		logPayload *commonpb.LedgerLogPayload
		err        error
	)
	switch applyData := apply.Data.(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		logPayload, err = p.processAddMetadata(apply.Ledger, boundaries, applyData.AddMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		logPayload, err = p.processDeleteMetadata(apply.Ledger, boundaries, applyData.DeleteMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		logPayload, err = p.processCreateTransaction(apply.Ledger, boundaries, applyData.CreateTransaction, s)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		logPayload, err = p.processRevertTransaction(apply.Ledger, boundaries, applyData.RevertTransaction, s)
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		logPayload, err = p.processSetMetadataFieldType(apply.Ledger, applyData.SetMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		logPayload, err = p.processRemoveMetadataFieldType(apply.Ledger, applyData.RemoveMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch:
		logPayload, err = p.processConvertMetadataBatch(apply.Ledger, applyData.ConvertMetadataBatch, s)
	case *raftcmdpb.LedgerApplyOrder_ConversionComplete:
		logPayload, err = p.processMetadataConversionComplete(apply.Ledger, applyData.ConversionComplete, s)
	case *raftcmdpb.LedgerApplyOrder_CreateIndex:
		logPayload, err = p.processCreateIndex(apply.Ledger, applyData.CreateIndex, s)
	case *raftcmdpb.LedgerApplyOrder_DropIndex:
		logPayload, err = p.processDropIndex(apply.Ledger, applyData.DropIndex, s)
	case *raftcmdpb.LedgerApplyOrder_IndexReady:
		logPayload, err = p.processIndexReady(apply.Ledger, applyData.IndexReady, s)
	case *raftcmdpb.LedgerApplyOrder_SetChartOfAccounts:
		logPayload, err = p.processSetChartOfAccounts(apply.Ledger, applyData.SetChartOfAccounts, s)
	case *raftcmdpb.LedgerApplyOrder_SetChartEnforcementMode:
		logPayload, err = p.processSetChartEnforcementMode(apply.Ledger, applyData.SetChartEnforcementMode, s)
	default:
		return nil, fmt.Errorf("invalid apply type")
	}
	if err != nil {
		return nil, err
	}

	nextLogID := boundaries.NextLogId
	boundaries.NextLogId = nextLogID + 1

	s.PutBoundaries(apply.Ledger, boundaries)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: apply.Ledger,
				Log: &commonpb.LedgerLog{
					Data: logPayload,
					Date: s.GetDate(),
					Id:   nextLogID,
				},
			},
		},
	}, nil
}

// isMirrorSafeApply returns true if the apply order is safe to execute on a
// mirror-mode ledger. Schema operations (set/remove metadata field type and
// their associated conversion lifecycle) only affect local configuration and
// do not cause drift with the mirror source.
func isMirrorSafeApply(apply *raftcmdpb.LedgerApplyOrder) bool {
	switch apply.Data.(type) {
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType,
		*raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType,
		*raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch,
		*raftcmdpb.LedgerApplyOrder_ConversionComplete,
		*raftcmdpb.LedgerApplyOrder_CreateIndex,
		*raftcmdpb.LedgerApplyOrder_DropIndex,
		*raftcmdpb.LedgerApplyOrder_IndexReady:
		return true
	default:
		return false
	}
}
