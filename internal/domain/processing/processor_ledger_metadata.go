package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddLedgerMetadata(ledger string, order *raftcmdpb.SaveLedgerMetadataOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	if ledger == "" {
		return nil, domain.ErrLedgerNameRequired
	}

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	// Stored values are immutable and reads return them verbatim;
	// declared_type only governs forward-index encoding on the indexer
	// side. The indexer no longer needs the FSM-captured previous
	// values either — it resolves prior encoded values via the reverse
	// map at apply time.

	for key, value := range order.GetMetadata() {
		metaKey := domain.LedgerMetadataKey{
			LedgerName: info.GetName(),
			Key:        key,
		}

		s.PutLedgerMetadata(metaKey, value)
	}

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_SavedLedgerMetadata{
			SavedLedgerMetadata: &commonpb.SavedLedgerMetadataLog{
				Ledger:   ledger,
				Metadata: order.GetMetadata(),
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteLedgerMetadata(ledger string, order *raftcmdpb.DeleteLedgerMetadataOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
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

	// Existence check (METADATA_NOT_FOUND on miss). The stored value itself
	// is no longer captured into the log; the indexer resolves the old
	// encoded value via the reverse map at apply time.
	if _, err := s.GetLedgerMetadata(metaKey); err != nil {
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
				Ledger: ledger,
				Key:    order.GetKey(),
			},
		},
	}, nil
}
