package processing

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processApply(apply *raftcmdpb.LedgerApplyOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	// Check deletion status before boundaries: MarkLedgerForCleanup removes
	// boundaries on delete, so GetBoundaries would return false and we'd
	// incorrectly return ErrLedgerNotFound instead of ErrLedgerDeleted.
	ledgerInfoReader, infoOk := s.GetLedger(apply.GetLedger())
	if infoOk && ledgerInfoReader.GetDeletedAt() != nil {
		return nil, &domain.ErrLedgerDeleted{Name: apply.GetLedger()}
	}

	boundariesReader, ok := s.GetBoundaries(apply.GetLedger())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: apply.GetLedger()}
	}

	boundaries := boundariesReader.Mutate()

	// Block writes on mirror-mode ledgers.
	if infoOk && ledgerInfoReader.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR && !isMirrorSafeApply(apply) {
		return nil, &domain.ErrLedgerInMirrorMode{Name: apply.GetLedger()}
	}

	// Mutate() once at the boundary so sub-processors receive a *LedgerInfo
	// they can read freely. The clone is discarded if no PutLedger happens;
	// writes to the cache still go through s.PutLedger.
	var ledgerInfo *commonpb.LedgerInfo
	if infoOk {
		ledgerInfo = ledgerInfoReader.Mutate()
	}

	var (
		logPayload *commonpb.LedgerLogPayload
		err        domain.Describable
	)

	ledgerName := apply.GetLedger()

	switch applyData := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		logPayload, err = p.processAddMetadata(ledgerName, boundaries, applyData.AddMetadata, s, ledgerInfo)
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		logPayload, err = p.processDeleteMetadata(ledgerName, boundaries, applyData.DeleteMetadata, s, ledgerInfo)
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		logPayload, err = p.processCreateTransaction(ledgerName, boundaries, applyData.CreateTransaction, s, ledgerInfo)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		logPayload, err = p.processRevertTransaction(ledgerName, boundaries, applyData.RevertTransaction, s, ledgerInfo)
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		logPayload, err = p.processSetMetadataFieldType(apply.GetLedger(), applyData.SetMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		logPayload, err = p.processRemoveMetadataFieldType(apply.GetLedger(), applyData.RemoveMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_CreateIndex:
		logPayload, err = p.processCreateIndex(apply.GetLedger(), applyData.CreateIndex, s)
	case *raftcmdpb.LedgerApplyOrder_DropIndex:
		logPayload, err = p.processDropIndex(apply.GetLedger(), applyData.DropIndex, s)
	case *raftcmdpb.LedgerApplyOrder_AddAccountType:
		logPayload, err = p.processAddAccountType(apply.GetLedger(), applyData.AddAccountType, s)
	case *raftcmdpb.LedgerApplyOrder_RemoveAccountType:
		logPayload, err = p.processRemoveAccountType(apply.GetLedger(), applyData.RemoveAccountType, s)
	case *raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode:
		logPayload, err = p.processUpdateDefaultEnforcementMode(apply.GetLedger(), applyData.UpdateDefaultEnforcementMode, s)
	default:
		return nil, &domain.ErrInvalidApplyType{TypeName: fmt.Sprintf("%T", apply.GetData())}
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
		*raftcmdpb.LedgerApplyOrder_CreateIndex,
		*raftcmdpb.LedgerApplyOrder_DropIndex,
		*raftcmdpb.LedgerApplyOrder_AddAccountType,
		*raftcmdpb.LedgerApplyOrder_RemoveAccountType,
		*raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode:
		return true
	default:
		return false
	}
}
