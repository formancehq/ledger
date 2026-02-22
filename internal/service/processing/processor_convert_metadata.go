package processing

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func (p *RequestProcessor) processConvertMetadataBatch(
	ledgerName string,
	order *raftcmdpb.ConvertMetadataBatchOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &ErrLedgerNotFound{Name: ledgerName}
	}

	// Staleness check: the field must still be CONVERTING with the expected type.
	_, fieldSchema := schemaFieldForTarget(info.MetadataSchema, order.TargetType, order.Key)
	if fieldSchema == nil ||
		fieldSchema.Status != commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
		fieldSchema.Type != order.ExpectedType {
		// Stale batch: schema was changed or removed since this order was created.
		return &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_ConvertMetadataBatch{
				ConvertMetadataBatch: &commonpb.ConvertMetadataBatchLog{
					TargetType: order.TargetType,
					Key:        order.Key,
					Count:      0,
				},
			},
		}, nil
	}

	var count uint32
	for _, entry := range order.Entries {
		var mk dal.MetadataKey
		if err := mk.Unmarshal(entry.CanonicalKey); err != nil {
			return nil, fmt.Errorf("unmarshal metadata key: %w", err)
		}

		value, err := s.GetAccountMetadata(mk)
		if err != nil {
			// Key may have been deleted since the scan; skip.
			continue
		}

		// Only overwrite if the value still needs conversion.
		if !commonpb.TypeMatches(value, order.ExpectedType) {
			s.PutAccountMetadata(mk, entry.ConvertedValue)
			count++
		}
	}

	// Persist conversion progress in the schema.
	fieldSchema.TotalKeys = order.TotalKeys
	fieldSchema.ConvertedKeys = order.ConvertedKeysSoFar
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_ConvertMetadataBatch{
			ConvertMetadataBatch: &commonpb.ConvertMetadataBatchLog{
				TargetType: order.TargetType,
				Key:        order.Key,
				Count:      count,
			},
		},
	}, nil
}

func (p *RequestProcessor) processMetadataConversionComplete(
	ledgerName string,
	order *raftcmdpb.MetadataConversionCompleteOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &ErrLedgerNotFound{Name: ledgerName}
	}

	// Staleness check: the field must still be CONVERTING with the expected type.
	_, fieldSchema := schemaFieldForTarget(info.MetadataSchema, order.TargetType, order.Key)
	if fieldSchema == nil ||
		fieldSchema.Status != commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
		fieldSchema.Type != order.ExpectedType {
		// Stale: schema was changed or removed since conversion started.
		return &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_MetadataConversionComplete{
				MetadataConversionComplete: &commonpb.MetadataConversionCompleteLog{
					TargetType: order.TargetType,
					Key:        order.Key,
				},
			},
		}, nil
	}

	fieldSchema.Status = commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE
	fieldSchema.ConvertedKeys = fieldSchema.TotalKeys
	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_MetadataConversionComplete{
			MetadataConversionComplete: &commonpb.MetadataConversionCompleteLog{
				TargetType: order.TargetType,
				Key:        order.Key,
			},
		},
	}, nil
}
