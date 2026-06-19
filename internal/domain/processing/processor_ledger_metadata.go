package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddLedgerMetadata(order *raftcmdpb.SaveLedgerMetadataOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	ledger := order.GetLedger()
	if ledger == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	infoReader, ok := s.GetLedger(ledger)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledger}
	}

	info := infoReader.Mutate()
	enforceSchemaMap(info.GetMetadataSchema(), commonpb.TargetType_TARGET_TYPE_LEDGER, order.GetMetadata())

	var previousValues map[string]*commonpb.MetadataValue

	for key, value := range order.GetMetadata() {
		metaKey := domain.LedgerMetadataKey{
			LedgerName: info.GetName(),
			Key:        key,
		}

		if oldVal, err := s.GetLedgerMetadata(metaKey); err == nil && oldVal != nil {
			if previousValues == nil {
				previousValues = make(map[string]*commonpb.MetadataValue)
			}

			previousValues[key] = coerceToDeclaredType(info.GetMetadataSchema(), commonpb.TargetType_TARGET_TYPE_LEDGER, key, oldVal.Mutate())
		}

		s.PutLedgerMetadata(metaKey, value)
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SavedLedgerMetadata{
			SavedLedgerMetadata: &commonpb.SavedLedgerMetadataLog{
				Ledger:         ledger,
				Metadata:       order.GetMetadata(),
				PreviousValues: previousValues,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteLedgerMetadata(order *raftcmdpb.DeleteLedgerMetadataOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	ledger := order.GetLedger()
	if ledger == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	if order.GetKey() == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

	infoReader, ok := s.GetLedger(ledger)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledger}
	}

	info := infoReader.Mutate()

	metaKey := domain.LedgerMetadataKey{
		LedgerName: info.GetName(),
		Key:        order.GetKey(),
	}

	oldVal, err := s.GetLedgerMetadata(metaKey)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, &domain.ErrMetadataNotFound{
				Target: ledger,
				Key:    order.GetKey(),
			}
		}

		return nil, &domain.ErrStorageOperation{Operation: "checking ledger metadata", Cause: err}
	}

	s.DeleteLedgerMetadata(metaKey)

	var previousValue *commonpb.MetadataValue
	if oldVal != nil {
		previousValue = oldVal.Mutate()
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedLedgerMetadata{
			DeletedLedgerMetadata: &commonpb.DeletedLedgerMetadataLog{
				Ledger:        ledger,
				Key:           order.GetKey(),
				PreviousValue: coerceToDeclaredType(info.GetMetadataSchema(), commonpb.TargetType_TARGET_TYPE_LEDGER, order.GetKey(), previousValue),
			},
		},
	}, nil
}
