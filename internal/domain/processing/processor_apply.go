package processing

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processApply(ledger string, apply *raftcmdpb.LedgerApplyOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	// Check deletion status before boundaries: MarkLedgerForCleanup removes
	// boundaries on delete, so loadBoundaries would surface ErrLedgerNotFound
	// even though the ledger is just deleted.
	ledgerInfo, infoErr := s.GetLedger(ledger)
	if infoErr != nil && !errors.Is(infoErr, domain.ErrNotFound) {
		return nil, &domain.ErrStorageOperation{Operation: "loading ledger", Cause: infoErr}
	}

	infoOk := infoErr == nil
	if infoOk && ledgerInfo.GetDeletedAt() != nil {
		return nil, &domain.ErrLedgerDeleted{Name: ledger}
	}

	boundariesReader, loadErr := loadBoundaries(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	boundaries := boundariesReader.Mutate()

	// Block writes on mirror-mode ledgers.
	if infoOk && ledgerInfo.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR && !isMirrorSafeApply(apply) {
		return nil, &domain.ErrLedgerInMirrorMode{Name: ledger}
	}

	var (
		logPayload *commonpb.LedgerLogPayload
		err        domain.Describable
	)

	switch applyData := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		logPayload, err = p.processAddMetadata(ledger, boundaries, applyData.AddMetadata, s, ledgerInfo)
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		logPayload, err = p.processDeleteMetadata(ledger, boundaries, applyData.DeleteMetadata, s, ledgerInfo)
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		logPayload, err = p.processCreateTransaction(ledger, boundaries, applyData.CreateTransaction, s, ledgerInfo)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		logPayload, err = p.processRevertTransaction(ledger, boundaries, applyData.RevertTransaction, s, ledgerInfo)
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		logPayload, err = p.processSetMetadataFieldType(ledger, applyData.SetMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		logPayload, err = p.processRemoveMetadataFieldType(ledger, applyData.RemoveMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_CreateIndex:
		logPayload, err = p.processCreateIndex(ledger, applyData.CreateIndex, s)
	case *raftcmdpb.LedgerApplyOrder_DropIndex:
		logPayload, err = p.processDropIndex(ledger, applyData.DropIndex, s)
	case *raftcmdpb.LedgerApplyOrder_AddAccountType:
		logPayload, err = p.processAddAccountType(ledger, applyData.AddAccountType, s)
	case *raftcmdpb.LedgerApplyOrder_RemoveAccountType:
		logPayload, err = p.processRemoveAccountType(ledger, applyData.RemoveAccountType, s)
	case *raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode:
		logPayload, err = p.processUpdateDefaultEnforcementMode(ledger, applyData.UpdateDefaultEnforcementMode, s)
	default:
		return nil, &domain.ErrInvalidApplyType{TypeName: fmt.Sprintf("%T", apply.GetData())}
	}

	if err != nil {
		return nil, err
	}

	nextLogID := boundaries.GetNextLogId()
	boundaries.NextLogId = nextLogID + 1

	s.PutBoundaries(ledger, boundaries)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
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
