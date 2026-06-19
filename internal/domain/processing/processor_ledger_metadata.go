package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddLedgerMetadata(order *raftcmdpb.SaveLedgerMetadataOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	ledger := order.GetLedger()
	if ledger == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	enforceSchemaMap(info.GetMetadataSchema(), commonpb.TargetType_TARGET_TYPE_LEDGER, order.GetMetadata())

	var previousValues map[string]*commonpb.MetadataValue

	for key, value := range order.GetMetadata() {
		metaKey := domain.LedgerMetadataKey{
			LedgerName: info.GetName(),
			Key:        key,
		}

		oldVal, err := s.GetLedgerMetadata(metaKey)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, &domain.ErrStorageOperation{Operation: "reading previous ledger metadata", Cause: err}
		}

		if err == nil {
			if previousValues == nil {
				previousValues = make(map[string]*commonpb.MetadataValue)
			}

			previousValues[key] = oldVal
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

func (p *RequestProcessor) processDeleteLedgerMetadata(order *raftcmdpb.DeleteLedgerMetadataOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	ledger := order.GetLedger()
	if ledger == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	if order.GetKey() == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

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

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_DeletedLedgerMetadata{
			DeletedLedgerMetadata: &commonpb.DeletedLedgerMetadataLog{
				Ledger:        ledger,
				Key:           order.GetKey(),
				PreviousValue: oldVal,
			},
		},
	}, nil
}
