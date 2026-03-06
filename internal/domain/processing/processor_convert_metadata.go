package processing

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processConvertMetadataBatch(
	ledgerName string,
	order *raftcmdpb.ConvertMetadataBatchOrder,
	s InMemoryStore,
) (*commonpb.LedgerLogPayload, error) {
	info, ok := s.GetLedger(ledgerName)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	// Staleness check: the field must still be CONVERTING with the expected type.
	_, fieldSchema := schemaFieldForTarget(info.GetMetadataSchema(), order.GetTargetType(), order.GetKey())
	if fieldSchema == nil ||
		fieldSchema.GetStatus() != commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
		fieldSchema.GetType() != order.GetExpectedType() {
		// Stale batch: schema was changed or removed since this order was created.
		return &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_ConvertMetadataBatch{
				ConvertMetadataBatch: &commonpb.ConvertMetadataBatchLog{
					TargetType: order.GetTargetType(),
					Key:        order.GetKey(),
					Count:      0,
				},
			},
		}, nil
	}

	var count uint32

	for _, entry := range order.GetEntries() {
		var mk domain.MetadataKey
		if err := mk.Unmarshal(entry.GetCanonicalKey()); err != nil {
			return nil, fmt.Errorf("unmarshal metadata key: %w", err)
		}

		value, err := s.GetAccountMetadata(mk)
		if err != nil {
			// Key may have been deleted since the scan; skip.
			continue
		}

		// Only overwrite if the value still needs conversion.
		if !commonpb.TypeMatches(value, order.GetExpectedType()) {
			s.PutAccountMetadata(mk, entry.GetConvertedValue())

			count++
		}
	}

	// Persist conversion progress in the schema.
	fieldSchema.TotalKeys = order.GetTotalKeys()
	fieldSchema.ConvertedKeys = order.GetConvertedKeysSoFar()

	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_ConvertMetadataBatch{
			ConvertMetadataBatch: &commonpb.ConvertMetadataBatchLog{
				TargetType: order.GetTargetType(),
				Key:        order.GetKey(),
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
		return nil, &domain.ErrLedgerNotFound{Name: ledgerName}
	}

	// Staleness check: the field must still be CONVERTING with the expected type.
	_, fieldSchema := schemaFieldForTarget(info.GetMetadataSchema(), order.GetTargetType(), order.GetKey())
	if fieldSchema == nil ||
		fieldSchema.GetStatus() != commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
		fieldSchema.GetType() != order.GetExpectedType() {
		// Stale: schema was changed or removed since conversion started.
		return &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_MetadataConversionComplete{
				MetadataConversionComplete: &commonpb.MetadataConversionCompleteLog{
					TargetType: order.GetTargetType(),
					Key:        order.GetKey(),
				},
			},
		}, nil
	}

	fieldSchema.Status = commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE
	fieldSchema.ConvertedKeys = fieldSchema.GetTotalKeys()

	s.PutLedger(ledgerName, info)

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_MetadataConversionComplete{
			MetadataConversionComplete: &commonpb.MetadataConversionCompleteLog{
				TargetType: order.GetTargetType(),
				Key:        order.GetKey(),
			},
		},
	}, nil
}
