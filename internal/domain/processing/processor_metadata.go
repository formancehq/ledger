package processing

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/accounttype"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddMetadata(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.SaveMetadataOrder, s InMemoryStore) (*commonpb.LedgerLogPayload, error) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	// Load ledger info once for both chart validation and schema enforcement.
	var info *commonpb.LedgerInfo
	if ledgerInfo, ok := s.GetLedger(ledger); ok {
		info = ledgerInfo
	}

	// Validate account address against account types.
	if acct, isAcct := order.GetTarget().GetTarget().(*commonpb.Target_Account); isAcct && info != nil {
		if len(info.GetAccountTypes()) > 0 {
			compiled := accounttype.CompileTypes(info.GetAccountTypes())
			if typeErr := validateAccountAgainstAccountTypes(acct.Account.GetAddr(), compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
				return nil, typeErr
			}
		}
	}

	// Enforce schema: convert metadata values to declared types.
	if order.GetMetadata() != nil && info != nil && info.GetMetadataSchema() != nil {
		targetType := commonpb.TargetType_TARGET_TYPE_ACCOUNT
		if _, isTx := order.GetTarget().GetTarget().(*commonpb.Target_Transaction); isTx {
			targetType = commonpb.TargetType_TARGET_TYPE_TRANSACTION
		}

		enforceSchema(info.GetMetadataSchema(), targetType, order.GetMetadata().GetMetadata())
	}

	var previousValues map[string]*commonpb.MetadataValue

	switch target := order.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		for _, entry := range order.GetMetadata().GetMetadata() {
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{
					Ledger:  ledger,
					Account: target.Account.GetAddr(),
				},
				Key: entry.GetKey(),
			}

			// Capture old value before overwriting (for log replay in indexbuilder).
			if oldVal, err := s.GetAccountMetadata(metaKey); err == nil {
				if previousValues == nil {
					previousValues = make(map[string]*commonpb.MetadataValue)
				}

				previousValues[entry.GetKey()] = oldVal
			}

			s.PutAccountMetadata(metaKey, entry.GetValue())
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
			state.Metadata = &commonpb.MetadataSet{}
		}

		for _, metadatum := range order.GetMetadata().GetMetadata() {
			// Replace existing key or append
			found := false

			for i, existing := range state.GetMetadata().GetMetadata() {
				if existing.GetKey() == metadatum.GetKey() {
					// Capture old value before overwriting.
					if previousValues == nil {
						previousValues = make(map[string]*commonpb.MetadataValue)
					}

					previousValues[metadatum.GetKey()] = existing.GetValue()
					state.Metadata.Metadata[i] = metadatum
					found = true

					break
				}
			}

			if !found {
				state.Metadata.Metadata = append(state.Metadata.Metadata, metadatum)
			}
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
			filtered := make([]*commonpb.Metadata, 0, len(state.GetMetadata().GetMetadata()))

			for _, md := range state.GetMetadata().GetMetadata() {
				if md.GetKey() == order.GetKey() {
					previousValue = md.GetValue()
				} else {
					filtered = append(filtered, md)
				}
			}

			state.Metadata.Metadata = filtered
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
