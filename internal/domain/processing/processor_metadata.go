package processing

import (
	"errors"
	"maps"
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

	// Stored values are immutable to background processes; the FSM stores what
	// the client sent verbatim and reads return those bytes as-is — the
	// declared type is an index hint, not an API contract. The indexer
	// resolves the old encoded value via the reverse map on overwrite, so
	// the FSM no longer captures previous values for it.

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

		maps.Copy(state.GetMetadata(), order.GetMetadata())

		s.PutTransactionState(txKey, state)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{
				Target:   loggedTarget,
				Metadata: order.GetMetadata(),
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteMetadata(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.DeleteMetadataOrder, s Scope) (*commonpb.LedgerLogPayload, domain.Describable) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	if order.GetKey() == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

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

		// Existence check (METADATA_NOT_FOUND on miss) — the stored value
		// itself is no longer captured into the log; the indexer resolves
		// the old encoded value via the reverse map on apply.
		if _, err := s.GetAccountMetadata(metaKey); err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrMetadataNotFound{
					Target: target.Account.GetAddr(),
					Key:    order.GetKey(),
				}
			}

			return nil, &domain.ErrStorageOperation{Operation: "checking account metadata", Cause: err}
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
		if _, ok := state.GetMetadata()[order.GetKey()]; !ok {
			return nil, &domain.ErrMetadataNotFound{
				Target: strconv.FormatUint(txID, 10),
				Key:    order.GetKey(),
			}
		}

		delete(state.GetMetadata(), order.GetKey())
		s.PutTransactionState(txKey, state)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{
				Target: loggedTarget,
				Key:    order.GetKey(),
			},
		},
	}, nil
}
