package processing

import (
	"errors"
	"strconv"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// validateTransactionTarget verifies that txID is non-zero and below the
// ledger's next-id boundary. Returns the proper Describable on failure.
func validateTransactionTarget(txID uint64, boundaries *raftcmdpb.LedgerBoundaries) domain.Describable {
	if txID == 0 {
		return domain.ErrTransactionTargetMissing
	}

	if txID >= boundaries.GetNextTransactionId() {
		return &domain.ErrTransactionNotFound{TransactionID: txID}
	}

	return nil
}

func (p *RequestProcessor) processAddMetadata(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.SaveMetadataOrder, s Scope, info *commonpb.LedgerInfo) (*commonpb.LedgerLogPayload, domain.Describable) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	loggedTarget := order.GetTarget()

	// Validate account address against account types.
	if acct, isAcct := order.GetTarget().GetTarget().(*commonpb.Target_Account); isAcct {
		if compiled := p.getCompiledTypes(ledgerName, info); len(compiled) > 0 {
			if typeErr := validateAccountAgainstAccountTypes(acct.Account.GetAddr(), compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
				return nil, typeErr
			}
		}
	}

	// Enforce schema: convert metadata values to declared types.
	if len(order.GetMetadata()) > 0 && info != nil && info.GetMetadataSchema() != nil {
		targetType := commonpb.TargetType_TARGET_TYPE_ACCOUNT
		if _, isTx := order.GetTarget().GetTarget().(*commonpb.Target_TransactionId); isTx {
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
					LedgerName: ledgerName,
					Account:    target.Account.GetAddr(),
				},
				Key: key,
			}

			// Capture old value before overwriting (for log replay in indexbuilder).
			oldVal, err := s.GetAccountMetadata(metaKey)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrStorageOperation{Operation: "reading previous account metadata", Cause: err}
			}

			if err == nil {
				if previousValues == nil {
					previousValues = make(map[string]*commonpb.MetadataValue)
				}

				previousValues[key] = oldVal
			}

			s.PutAccountMetadata(metaKey, value)
		}
	case *commonpb.Target_TransactionId:
		txID := target.TransactionId
		if resolveErr := validateTransactionTarget(txID, boundaries); resolveErr != nil {
			return nil, resolveErr
		}

		txKey := domain.TransactionKey{LedgerName: ledgerName, ID: txID}

		state, err := s.GetTransactionState(txKey)
		if err != nil {
			return nil, &domain.ErrStorageOperation{Operation: "getting transaction state", Cause: err}
		}

		if state == nil {
			return nil, &domain.ErrTransactionNotFound{TransactionID: txID}
		}

		// Clone before mutating — GetTransactionState may return the
		// cached proto pointer when the key falls through to the parent
		// KeyStore. Mutating its Metadata map in place would write
		// through the cache before the proposal reached Merge.
		state = state.CloneVT()

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
				Target:         loggedTarget,
				Metadata:       order.GetMetadata(),
				PreviousValues: previousValues,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteMetadata(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.DeleteMetadataOrder, s Scope, info *commonpb.LedgerInfo) (*commonpb.LedgerLogPayload, domain.Describable) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	if order.GetKey() == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

	var previousValue *commonpb.MetadataValue

	loggedTarget := order.GetTarget()

	switch target := order.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		metaKey := domain.MetadataKey{
			AccountKey: domain.AccountKey{
				LedgerName: ledgerName,
				Account:    target.Account.GetAddr(),
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

			return nil, &domain.ErrStorageOperation{Operation: "checking account metadata", Cause: err}
		}

		if oldVal != nil {
			previousValue = coerceToDeclaredType(info.GetMetadataSchema(), commonpb.TargetType_TARGET_TYPE_ACCOUNT, order.GetKey(), oldVal)
		}

		s.DeleteAccountMetadata(metaKey)
	case *commonpb.Target_TransactionId:
		txID := target.TransactionId
		if resolveErr := validateTransactionTarget(txID, boundaries); resolveErr != nil {
			return nil, resolveErr
		}

		txKey := domain.TransactionKey{LedgerName: ledgerName, ID: txID}

		state, err := s.GetTransactionState(txKey)
		if err != nil {
			return nil, &domain.ErrStorageOperation{Operation: "getting transaction state for delete", Cause: err}
		}

		if state == nil {
			return nil, &domain.ErrTransactionNotFound{TransactionID: txID}
		}

		// Clone before mutating — GetTransactionState may return the
		// cached proto pointer when the key falls through to the parent
		// KeyStore. See the matching note in processSaveMetadata.
		state = state.CloneVT()

		// Reject a missing key with METADATA_NOT_FOUND (master #492):
		// callers that delete a key they never set get an explicit
		// rejection instead of a silent skip.
		val, ok := state.GetMetadata()[order.GetKey()]
		if !ok {
			return nil, &domain.ErrMetadataNotFound{
				Target: strconv.FormatUint(txID, 10),
				Key:    order.GetKey(),
			}
		}

		previousValue = coerceToDeclaredType(info.GetMetadataSchema(), commonpb.TargetType_TARGET_TYPE_TRANSACTION, order.GetKey(), val)
		delete(state.GetMetadata(), order.GetKey())
		s.PutTransactionState(txKey, state)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{
				Target:        loggedTarget,
				Key:           order.GetKey(),
				PreviousValue: previousValue,
			},
		},
	}, nil
}
