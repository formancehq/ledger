package processing

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processApply(apply *raftcmdpb.LedgerApplyOrder, s Store) (*commonpb.LogPayload, error) {
	boundaries, ok := s.GetBoundaries(apply.Ledger)
	if !ok {
		return nil, &ErrLedgerNotFound{Name: apply.Ledger}
	}

	ledgerID := boundaries.LedgerId

	var (
		logPayload *commonpb.LedgerLogPayload
		err        error
	)
	switch applyData := apply.Data.(type) {
	case *raftcmdpb.LedgerApplyOrder_AddMetadata:
		logPayload, err = p.processAddMetadata(ledgerID, boundaries, apply.Ledger, applyData.AddMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_DeleteMetadata:
		logPayload, err = p.processDeleteMetadata(ledgerID, boundaries, applyData.DeleteMetadata, s)
	case *raftcmdpb.LedgerApplyOrder_CreateTransaction:
		logPayload, err = p.processCreateTransaction(ledgerID, boundaries, apply.Ledger, applyData.CreateTransaction, s)
	case *raftcmdpb.LedgerApplyOrder_RevertTransaction:
		logPayload, err = p.processRevertTransaction(ledgerID, boundaries, applyData.RevertTransaction, s)
	case *raftcmdpb.LedgerApplyOrder_SetMetadataFieldType:
		logPayload, err = p.processSetMetadataFieldType(apply.Ledger, applyData.SetMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType:
		logPayload, err = p.processRemoveMetadataFieldType(apply.Ledger, applyData.RemoveMetadataFieldType, s)
	case *raftcmdpb.LedgerApplyOrder_ConvertMetadataBatch:
		logPayload, err = p.processConvertMetadataBatch(apply.Ledger, applyData.ConvertMetadataBatch, s)
	case *raftcmdpb.LedgerApplyOrder_ConversionComplete:
		logPayload, err = p.processMetadataConversionComplete(apply.Ledger, applyData.ConversionComplete, s)
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
