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

	info = info.CloneVT()

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

	var (
		count      uint32
		logEntries []*commonpb.ConvertMetadataBatchLogEntry
	)

	for _, entry := range order.GetEntries() {
		value, err := getConvertBatchValue(s, order.GetTargetType(), entry.GetCanonicalKey())
		if err != nil {
			return nil, err
		}

		if value == nil {
			// Key was deleted since the scan; skip.
			continue
		}

		// Only overwrite if the value still needs conversion.
		if !commonpb.TypeMatches(value, order.GetExpectedType()) {
			if err := putConvertBatchValue(s, order.GetTargetType(), entry.GetCanonicalKey(), entry.GetConvertedValue()); err != nil {
				return nil, err
			}

			logEntries = append(logEntries, &commonpb.ConvertMetadataBatchLogEntry{
				CanonicalKey: entry.GetCanonicalKey(),
				NewValue:     entry.GetConvertedValue(),
			})

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
				Entries:    logEntries,
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

	info = info.CloneVT()

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

// getConvertBatchValue retrieves the current metadata value for a canonical key,
// dispatching to the correct store method based on target type.
// Returns (nil, nil) when the key no longer exists (deleted since the scan).
func getConvertBatchValue(s InMemoryStore, targetType commonpb.TargetType, canonicalKey []byte) (*commonpb.MetadataValue, error) {
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		var mk domain.MetadataKey
		if err := mk.Unmarshal(canonicalKey); err != nil {
			return nil, fmt.Errorf("unmarshal metadata key: %w", err)
		}

		v, err := s.GetAccountMetadata(mk)
		if err != nil {
			return nil, nil //nolint:nilerr // key deleted since scan
		}

		return v, nil
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		var lmk domain.LedgerMetadataKey
		if err := lmk.Unmarshal(canonicalKey); err != nil {
			return nil, fmt.Errorf("unmarshal ledger metadata key: %w", err)
		}

		v, err := s.GetLedgerMetadata(lmk)
		if err != nil {
			return nil, nil //nolint:nilerr // key deleted since scan
		}

		return v, nil
	default:
		return nil, nil
	}
}

// putConvertBatchValue stores a converted metadata value, dispatching to the
// correct store method based on target type.
func putConvertBatchValue(s InMemoryStore, targetType commonpb.TargetType, canonicalKey []byte, value *commonpb.MetadataValue) error {
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		var mk domain.MetadataKey
		if err := mk.Unmarshal(canonicalKey); err != nil {
			return fmt.Errorf("unmarshal metadata key: %w", err)
		}

		s.PutAccountMetadata(mk, value)
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		var lmk domain.LedgerMetadataKey
		if err := lmk.Unmarshal(canonicalKey); err != nil {
			return fmt.Errorf("unmarshal ledger metadata key: %w", err)
		}

		s.PutLedgerMetadata(lmk, value)
	}

	return nil
}
