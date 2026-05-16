package processing

import (
	"errors"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddLedgerMetadata(order *raftcmdpb.SaveLedgerMetadataOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	ledger := order.GetLedger()
	if ledger == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	info, ok := s.GetLedger(ledger)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledger}
	}

	enforceSchemaMap(info.GetMetadataSchema(), commonpb.TargetType_TARGET_TYPE_LEDGER, order.GetMetadata())

	var previousValues map[string]*commonpb.MetadataValue

	for key, value := range order.GetMetadata() {
		metaKey := domain.LedgerMetadataKey{
			Ledger: ledger,
			Key:    key,
		}

		if oldVal, err := s.GetLedgerMetadata(metaKey); err == nil {
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

func (p *RequestProcessor) processDeleteLedgerMetadata(order *raftcmdpb.DeleteLedgerMetadataOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	ledger := order.GetLedger()
	if ledger == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	if order.GetKey() == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

	_, ok := s.GetLedger(ledger)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: ledger}
	}

	metaKey := domain.LedgerMetadataKey{
		Ledger: ledger,
		Key:    order.GetKey(),
	}

	oldVal, err := s.GetLedgerMetadata(metaKey)
	if err != nil {
		if errors.Is(err, domain.ErrNotFound) {
			return nil, &domain.ErrMetadataNotFound{
				Target: ledger,
				Key:    order.GetKey(),
			}
		}

		return nil, err
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
