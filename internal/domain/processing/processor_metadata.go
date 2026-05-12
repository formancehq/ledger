package processing

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddMetadata(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.SaveMetadataOrder, s InMemoryStore, info *commonpb.LedgerInfo) (*commonpb.LedgerLogPayload, error) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	// Validate account address against account types.
	if acct, isAcct := order.GetTarget().GetTarget().(*commonpb.Target_Account); isAcct {
		if compiled := p.getCompiledTypes(ledger, info); len(compiled) > 0 {
			if typeErr := validateAccountAgainstAccountTypes(acct.Account.GetAddr(), compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
				return nil, typeErr
			}
		}
	}

	// Enforce schema: convert metadata values to declared types.
	if len(order.GetMetadata()) > 0 && info != nil && info.GetMetadataSchema() != nil {
		targetType := commonpb.TargetType_TARGET_TYPE_ACCOUNT
		if _, isTx := order.GetTarget().GetTarget().(*commonpb.Target_Transaction); isTx {
			targetType = commonpb.TargetType_TARGET_TYPE_TRANSACTION
		}

		enforceSchemaMap(info.GetMetadataSchema(), targetType, order.GetMetadata())
	}

	var previousValues map[string]*commonpb.MetadataValue

	switch target := order.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		for key, value := range order.GetMetadata() {
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{
					Ledger:  ledger,
					Account: target.Account.GetAddr(),
				},
				Key: key,
			}

			// Capture old value before overwriting (for log replay in indexbuilder).
			if oldVal, err := s.GetAccountMetadata(metaKey); err == nil {
				if previousValues == nil {
					previousValues = make(map[string]*commonpb.MetadataValue)
				}

				previousValues[key] = oldVal
			}

			s.PutAccountMetadata(metaKey, value)
		}
	case *commonpb.Target_Transaction:
		if target.Transaction.GetId() >= boundaries.GetNextTransactionId() {
			return nil, &domain.ErrTransactionNotFound{TransactionID: target.Transaction.GetId()}
		}

		txKey := domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}

		state, err := s.GetTransactionState(txKey)
		if err != nil {
			return nil, fmt.Errorf("getting transaction state: %w", err)
		}

		if state == nil {
			return nil, &domain.ErrTransactionNotFound{TransactionID: target.Transaction.GetId()}
		}

		// Add metadata entries to the transaction state
		if state.GetMetadata() == nil {
			state.Metadata = make(map[string]*commonpb.MetadataValue)
		}

		for key, value := range order.GetMetadata() {
			// Capture old value before overwriting.
			if existing, ok := state.GetMetadata()[key]; ok {
				if previousValues == nil {
					previousValues = make(map[string]*commonpb.MetadataValue)
				}

				previousValues[key] = existing
			}

			state.Metadata[key] = value
		}

		s.PutTransactionState(txKey, state)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{
				Target:         order.GetTarget(),
				Metadata:       order.GetMetadata(),
				PreviousValues: previousValues,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteMetadata(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.DeleteMetadataOrder, s InMemoryStore) (*commonpb.LedgerLogPayload, error) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	if order.GetKey() == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

	var previousValue *commonpb.MetadataValue

	switch target := order.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		metaKey := domain.MetadataKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledger,
				Account: target.Account.GetAddr(),
			},
			Key: order.GetKey(),
		}

		oldVal, err := s.GetAccountMetadata(metaKey)
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrMetadataNotFound{
					Target: target.Account.GetAddr(),
					Key:    order.GetKey(),
				}
			}

			return nil, fmt.Errorf("checking account metadata: %w", err)
		}

		previousValue = oldVal
		s.DeleteAccountMetadata(metaKey)
	case *commonpb.Target_Transaction:
		if target.Transaction.GetId() >= boundaries.GetNextTransactionId() {
			return nil, &domain.ErrTransactionNotFound{TransactionID: target.Transaction.GetId()}
		}

		txKey := domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}

		state, err := s.GetTransactionState(txKey)
		if err != nil {
			return nil, fmt.Errorf("getting transaction state for delete: %w", err)
		}

		if state == nil {
			return nil, &domain.ErrTransactionNotFound{TransactionID: target.Transaction.GetId()}
		}

		// Capture old value and remove the metadata key from the transaction state
		if state.GetMetadata() != nil {
			if val, ok := state.GetMetadata()[order.GetKey()]; ok {
				previousValue = val
				delete(state.GetMetadata(), order.GetKey())
			}
		}

		s.PutTransactionState(txKey, state)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{
				Target:        order.GetTarget(),
				Key:           order.GetKey(),
				PreviousValue: previousValue,
			},
		},
	}, nil
}
