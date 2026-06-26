package processing

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processApply is the orchestrator for ledger-scoped apply variants. It
// populates ctx.Boundaries and ctx.LedgerInfo before dispatching to apply-
// child handlers so children receive everything through a single uniform
// Context.
func processApply(ledger string, apply *raftcmdpb.LedgerApplyOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope

	// Check deletion status before boundaries: MarkLedgerForCleanup removes
	// boundaries on delete, so loadBoundaries would surface ErrLedgerNotFound
	// even though the ledger is just deleted.
	ledgerInfoReader, infoErr := s.Ledgers().Get(domain.LedgerKey{Name: ledger})
	if infoErr != nil && !errors.Is(infoErr, domain.ErrNotFound) {
		return nil, &domain.ErrStorageOperation{Operation: "loading ledger", Cause: infoErr}
	}

	infoOk := infoErr == nil
	if infoOk && ledgerInfoReader.GetDeletedAt() != nil {
		return nil, &domain.ErrLedgerDeleted{Name: ledger}
	}

	boundariesReader, loadErr := loadBoundaries(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	boundaries := boundariesReader.Mutate()

	// Block writes on mirror-mode ledgers.
	if infoOk && ledgerInfoReader.GetMode() == commonpb.LedgerMode_LEDGER_MODE_MIRROR && !isMirrorSafeApply(apply) {
		return nil, &domain.ErrLedgerInMirrorMode{Name: ledger}
	}

	// Mutate() once at the boundary so sub-processors keep receiving
	// *LedgerInfo via the per-apply context. The clone cost is bounded
	// (one CloneVT per apply).
	var ledgerInfo *commonpb.LedgerInfo
	if infoOk {
		ledgerInfo = ledgerInfoReader.Mutate()
	}

	// Stage per-apply context fields for child handlers.
	ctx.Boundaries = boundaries
	ctx.LedgerInfo = ledgerInfo

	var (
		logPayload *commonpb.LedgerLogPayload
		err        domain.Describable
	)

	switch applyData := apply.GetData().(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		logPayload, err = processAddMetadata(ledger, applyData.AddMetadata, ctx)
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		logPayload, err = processDeleteMetadata(ledger, applyData.DeleteMetadata, ctx)
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		logPayload, err = processCreateTransaction(ledger, applyData.CreateTransaction, ctx)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		logPayload, err = processRevertTransaction(ledger, applyData.RevertTransaction, ctx)
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		logPayload, err = processSetMetadataFieldType(ledger, applyData.SetMetadataFieldType, ctx)
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		logPayload, err = processRemoveMetadataFieldType(ledger, applyData.RemoveMetadataFieldType, ctx)
	case *raftcmdpb.LedgerApplyOrder_CreateIndex:
		logPayload, err = processCreateIndex(ledger, applyData.CreateIndex, ctx)
	case *raftcmdpb.LedgerApplyOrder_DropIndex:
		logPayload, err = processDropIndex(ledger, applyData.DropIndex, ctx)
	case *raftcmdpb.LedgerApplyOrder_AddAccountType:
		logPayload, err = processAddAccountType(ledger, applyData.AddAccountType, ctx)
	case *raftcmdpb.LedgerApplyOrder_RemoveAccountType:
		logPayload, err = processRemoveAccountType(ledger, applyData.RemoveAccountType, ctx)
	case *raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode:
		logPayload, err = processUpdateDefaultEnforcementMode(ledger, applyData.UpdateDefaultEnforcementMode, ctx)
	default:
		return nil, &domain.ErrInvalidApplyType{TypeName: fmt.Sprintf("%T", apply.GetData())}
	}

	if err != nil {
		return nil, err
	}

	nextLogID := boundaries.GetNextLogId()
	boundaries.NextLogId = nextLogID + 1

	s.Boundaries().Put(domain.LedgerKey{Name: ledger}, boundaries)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log: &commonpb.LedgerLog{
					Data: logPayload,
					Date: s.GetDate().Mutate(),
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
