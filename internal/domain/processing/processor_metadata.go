package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// canonicalizeTarget returns a Target whose Transaction identifier is always
// expressed as a numeric id, even when the caller supplied a reference. The
// log payload stored downstream (sinks, indexbuilder, replay, mirror, audit)
// reads `Target.Transaction.GetId()` directly; without this rewrite, every
// consumer would see id=0 for reference-typed targets and silently corrupt
// derived state. The user's original reference is dropped from the log; if
// it matters downstream, callers should embed it in the metadata payload.
func canonicalizeTarget(target *commonpb.Target, resolvedTxID uint64) *commonpb.Target {
	if target == nil {
		return nil
	}

	tx, ok := target.GetTarget().(*commonpb.Target_Transaction)
	if !ok || tx.Transaction == nil {
		return target
	}

	if _, isRef := tx.Transaction.GetIdentifier().(*commonpb.TargetTransaction_Reference); !isRef {
		return target
	}

	return &commonpb.Target{
		Target: &commonpb.Target_Transaction{
			Transaction: &commonpb.TargetTransaction{
				Identifier: &commonpb.TargetTransaction_Id{Id: resolvedTxID},
			},
		},
	}
}

// resolveTargetTransactionID returns the numeric transaction id for the given
// TargetTransaction. When the target carries a reference, it is resolved
// against the in-memory transaction reference index (which sees writes from
// previous orders in the same batch, including just-created transactions).
// The id path additionally validates against the ledger boundary; the
// reference path does not need that check because PutTransactionReference is
// only set when a transaction is actually allocated.
func resolveTargetTransactionID(t *commonpb.TargetTransaction, ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, s InMemoryStore) (uint64, domain.Describable) {
	switch id := t.GetIdentifier().(type) {
	case *commonpb.TargetTransaction_Id:
		if id.Id >= boundaries.GetNextTransactionId() {
			return 0, &domain.ErrTransactionNotFound{TransactionID: id.Id}
		}

		return id.Id, nil
	case *commonpb.TargetTransaction_Reference:
		if id.Reference == "" {
			return 0, domain.ErrTransactionTargetMissing
		}

		value, err := s.GetTransactionReference(domain.TransactionReferenceKey{LedgerName: ledgerName, Reference: id.Reference})
		if err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return 0, &domain.ErrTransactionReferenceNotFound{Reference: id.Reference}
			}

			return 0, &domain.ErrStorageOperation{Operation: "resolving transaction reference", Cause: err}
		}

		if value == nil {
			return 0, &domain.ErrTransactionReferenceNotFound{Reference: id.Reference}
		}

		return value.GetTransactionId(), nil
	default:
		return 0, domain.ErrTransactionTargetMissing
	}
}

func (p *RequestProcessor) processAddMetadata(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.SaveMetadataOrder, s InMemoryStore, info *commonpb.LedgerInfo) (*commonpb.LedgerLogPayload, domain.Describable) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	// loggedTarget defaults to the original target; the Target_Transaction
	// branch rewrites it to a canonical id-based variant so the log stays
	// usable across downstream consumers.
	loggedTarget := order.GetTarget()

	// Validate account address against account types.
	if acct, isAcct := order.GetTarget().GetTarget().(*commonpb.Target_Account); isAcct {
		if compiled := p.getCompiledTypes(ledgerName, info); len(compiled) > 0 {
			if typeErr := validateAccountAgainstAccountTypes(acct.Account.GetAddr(), compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
				return nil, typeErr
			}
		}
	}

	targetType := commonpb.TargetType_TARGET_TYPE_ACCOUNT
	if _, isTx := order.GetTarget().GetTarget().(*commonpb.Target_Transaction); isTx {
		targetType = commonpb.TargetType_TARGET_TYPE_TRANSACTION
	}

	// Enforce schema: convert metadata values to declared types.
	enforceSchemaMap(info.GetMetadataSchema(), targetType, order.GetMetadata())

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

			// Capture old value before overwriting; the log records it for downstream consumers.
			// Coerce it to the declared type so it matches what a read returned,
			// regardless of whether the background conversion rewrote it yet.
			if oldVal, err := s.GetAccountMetadata(metaKey); err == nil && oldVal != nil {
				if previousValues == nil {
					previousValues = make(map[string]*commonpb.MetadataValue)
				}

				previousValues[key] = coerceToDeclaredType(info.GetMetadataSchema(), targetType, key, oldVal.Mutate())
			}

			s.PutAccountMetadata(metaKey, value)
		}
	case *commonpb.Target_Transaction:
		txID, resolveErr := resolveTargetTransactionID(target.Transaction, ledgerName, boundaries, s)
		if resolveErr != nil {
			return nil, resolveErr
		}

		loggedTarget = canonicalizeTarget(order.GetTarget(), txID)

		txKey := domain.TransactionKey{LedgerName: ledgerName, ID: txID}

		stateReader, err := s.GetTransactionState(txKey)
		if err != nil {
			return nil, &domain.ErrStorageOperation{Operation: "getting transaction state", Cause: err}
		}

		if stateReader == nil {
			return nil, &domain.ErrTransactionNotFound{TransactionID: txID}
		}

		state := stateReader.Mutate()

		// Add metadata entries to the transaction state
		if state.GetMetadata() == nil {
			state.Metadata = make(map[string]*commonpb.MetadataValue)
		}

		for key, value := range order.GetMetadata() {
			// Capture old value before overwriting, coerced to the declared type
			// so it matches what a read returned, independent of conversion progress.
			if existing, ok := state.GetMetadata()[key]; ok {
				if previousValues == nil {
					previousValues = make(map[string]*commonpb.MetadataValue)
				}

				previousValues[key] = coerceToDeclaredType(info.GetMetadataSchema(), targetType, key, existing)
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

func (p *RequestProcessor) processDeleteMetadata(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.DeleteMetadataOrder, s InMemoryStore, info *commonpb.LedgerInfo) (*commonpb.LedgerLogPayload, domain.Describable) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	if order.GetKey() == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

	var previousValue *commonpb.MetadataValue

	// loggedTarget defaults to the original target; the Target_Transaction
	// branch rewrites it to a canonical id-based variant so the log stays
	// usable across downstream consumers.
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
			previousValue = coerceToDeclaredType(info.GetMetadataSchema(), commonpb.TargetType_TARGET_TYPE_ACCOUNT, order.GetKey(), oldVal.Mutate())
		}
		s.DeleteAccountMetadata(metaKey)
	case *commonpb.Target_Transaction:
		txID, resolveErr := resolveTargetTransactionID(target.Transaction, ledgerName, boundaries, s)
		if resolveErr != nil {
			return nil, resolveErr
		}

		loggedTarget = canonicalizeTarget(order.GetTarget(), txID)

		txKey := domain.TransactionKey{LedgerName: ledgerName, ID: txID}

		stateReader, err := s.GetTransactionState(txKey)
		if err != nil {
			return nil, &domain.ErrStorageOperation{Operation: "getting transaction state for delete", Cause: err}
		}

		if stateReader == nil {
			return nil, &domain.ErrTransactionNotFound{TransactionID: txID}
		}

		state := stateReader.Mutate()

		// Capture old value and remove the metadata key from the transaction state
		if state.GetMetadata() != nil {
			if val, ok := state.GetMetadata()[order.GetKey()]; ok {
				previousValue = coerceToDeclaredType(info.GetMetadataSchema(), commonpb.TargetType_TARGET_TYPE_TRANSACTION, order.GetKey(), val)
				delete(state.GetMetadata(), order.GetKey())
			}
		}

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
