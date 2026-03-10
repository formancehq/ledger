package processing

import (
	"errors"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processApply(apply *raftcmdpb.LedgerApplyOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	boundaries, ok := s.GetBoundaries(apply.GetLedger())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: apply.GetLedger()}
	}

	// Block data-modifying writes on mirror-mode ledgers.
	// Schema operations (set/remove metadata field type and their conversion
	// lifecycle) are allowed because they only affect local configuration,
	// not replicated data.
	if ledgerInfo, infoOk := s.GetLedger(apply.GetLedger()); infoOk && ledgerInfo.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		if !isMirrorSafeApply(apply) {
			return nil, &domain.ErrLedgerInMirrorMode{Name: apply.GetLedger()}
		}
	}

	var (
		logPayload *commonpb.LedgerLogPayload
		err        error
	)

	switch applyData := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		logPayload, err = p.processAddMetadata(apply.GetLedger(), boundaries, applyData.AddMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		logPayload, err = p.processDeleteMetadata(apply.GetLedger(), boundaries, applyData.DeleteMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		logPayload, err = p.processCreateTransaction(apply.GetLedger(), boundaries, applyData.CreateTransaction, s)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		logPayload, err = p.processRevertTransaction(apply.GetLedger(), boundaries, applyData.RevertTransaction, s)
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		logPayload, err = p.processSetMetadataFieldType(apply.GetLedger(), applyData.SetMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		logPayload, err = p.processRemoveMetadataFieldType(apply.GetLedger(), applyData.RemoveMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch:
		logPayload, err = p.processConvertMetadataBatch(apply.GetLedger(), applyData.ConvertMetadataBatch, s)
	case *raftcmdpb.LedgerApplyOrder_ConversionComplete:
		logPayload, err = p.processMetadataConversionComplete(apply.GetLedger(), applyData.ConversionComplete, s)
	case *raftcmdpb.LedgerApplyOrder_CreateIndex:
		logPayload, err = p.processCreateIndex(apply.GetLedger(), applyData.CreateIndex, s)
	case *raftcmdpb.LedgerApplyOrder_DropIndex:
		logPayload, err = p.processDropIndex(apply.GetLedger(), applyData.DropIndex, s)
	case *raftcmdpb.LedgerApplyOrder_IndexReady:
		logPayload, err = p.processIndexReady(apply.GetLedger(), applyData.IndexReady, s)
	case *raftcmdpb.LedgerApplyOrder_AddAccountType:
		logPayload, err = p.processAddAccountType(apply.GetLedger(), applyData.AddAccountType, s)
	case *raftcmdpb.LedgerApplyOrder_UpdateAccountType:
		logPayload, err = p.processUpdateAccountType(apply.GetLedger(), applyData.UpdateAccountType, s)
	case *raftcmdpb.LedgerApplyOrder_RemoveAccountType:
		logPayload, err = p.processRemoveAccountType(apply.GetLedger(), applyData.RemoveAccountType, s)
	default:
		return nil, errors.New("invalid apply type")
	}

	if err != nil {
		return nil, err
	}

	nextLogID := boundaries.GetNextLogId()
	boundaries.NextLogId = nextLogID + 1

	s.PutBoundaries(apply.GetLedger(), boundaries)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: apply.GetLedger(),
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
	switch apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType,
		*raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType,
		*raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch,
		*raftcmdpb.LedgerApplyOrder_ConversionComplete,
		*raftcmdpb.LedgerApplyOrder_CreateIndex,
		*raftcmdpb.LedgerApplyOrder_DropIndex,
		*raftcmdpb.LedgerApplyOrder_IndexReady,
		*raftcmdpb.LedgerApplyOrder_AddAccountType,
		*raftcmdpb.LedgerApplyOrder_UpdateAccountType,
		*raftcmdpb.LedgerApplyOrder_RemoveAccountType:
		return true
	default:
		return false
	}
}
