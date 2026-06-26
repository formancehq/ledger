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

func processAddMetadata(ledger string, order *raftcmdpb.SaveMetadataOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	boundaries := ctx.Boundaries
	s := ctx.Scope
	info := ctx.LedgerInfo

	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	loggedTarget := order.GetTarget()

	// Validate account address against account types.
	if acct, isAcct := order.GetTarget().GetTarget().(*commonpb.Target_Account); isAcct {
		if compiled := compiledTypesFor(ctx.CompiledTypes, ledger, info); len(compiled) > 0 {
			if typeErr := validateAccountAgainstAccountTypes(acct.Account.GetAddr(), compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
				return nil, typeErr
			}
		}
	}

	// Metadata to apply and log. For an account target this is widened with
	// account-type defaults below when the metadata-set first creates the
	// account; for a transaction target it is the order metadata verbatim.
	loggedMetadata := order.GetMetadata()

	// Stored values are immutable to background processes; the FSM stores what
	// the client sent verbatim and reads return those bytes as-is — the
	// declared type is an index hint, not an API contract. The indexer
	// resolves the old encoded value via the reverse map on overwrite, so
	// the FSM no longer captures previous values for it.

	switch target := order.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		account := target.Account.GetAddr()

		// EN-1276: a metadata-set is an account-creation path. When this is the
		// first time the account is ever seen on a defaults-bearing ledger,
		// record the existence marker and merge the account type's
		// default_metadata for keys the caller did not set explicitly — so the
		// account materialises with its defaults exactly as it would via a
		// transaction. Explicit keys always win. The merged map is what we both
		// apply and log, so replay/rebuild reconstruct the same metadata; the
		// marker keeps the next touch from re-applying defaults.
		if ledgerHasAccountTypeDefaults(info) {
			compiled := compiledTypesFor(ctx.CompiledTypes, ledger, info)

			defaults, defErr := markNewAccountAndMatchDefaults(s, ledger, account, compiled)
			if defErr != nil {
				return nil, defErr
			}

			loggedMetadata = mergeFlatDefaults(loggedMetadata, defaults)
		}

		for key, value := range loggedMetadata {
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{
					LedgerName: ledger,
					Account:    account,
				},
				Key: key,
			}

			s.AccountMetadata().Put(metaKey, value)
		}
	case *commonpb.Target_TransactionId:
		txID := target.TransactionId
		if resolveErr := validateTransactionTarget(txID, boundaries); resolveErr != nil {
			return nil, resolveErr
		}

		txKey := domain.TransactionKey{LedgerName: ledger, ID: txID}

		stateReader, err := s.TransactionStates().Get(txKey)
		if errors.Is(err, domain.ErrNotFound) {
			return nil, &domain.ErrTransactionNotFound{TransactionID: txID}
		}

		if err != nil {
			return nil, &domain.ErrStorageOperation{Operation: "getting transaction state", Cause: err}
		}

		// Mutate() yields a fresh clone — handlers may freely modify the
		// returned proto without writing through the cache before Merge.
		state := stateReader.Mutate()

		// Add metadata entries to the transaction state
		if state.GetMetadata() == nil {
			state.Metadata = make(map[string]*commonpb.MetadataValue)
		}

		maps.Copy(state.GetMetadata(), order.GetMetadata())

		s.TransactionStates().Put(txKey, state)
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{
				Target:   loggedTarget,
				Metadata: loggedMetadata,
			},
		},
	}, nil
}

// mergeFlatDefaults returns `explicit` widened with `defaults` for keys not
// already present (explicit caller-supplied metadata always wins). explicit is
// never mutated — it may alias the order's own proto, so a fresh map is
// allocated (CloneVT discipline). A nil/empty `defaults` returns explicit
// unchanged so the common no-defaults path allocates nothing.
func mergeFlatDefaults(explicit, defaults map[string]*commonpb.MetadataValue) map[string]*commonpb.MetadataValue {
	if len(defaults) == 0 {
		return explicit
	}

	merged := make(map[string]*commonpb.MetadataValue, len(explicit)+len(defaults))
	maps.Copy(merged, explicit)

	for key, value := range defaults {
		if _, set := merged[key]; !set {
			merged[key] = value
		}
	}

	return merged
}

func processDeleteMetadata(ledger string, order *raftcmdpb.DeleteMetadataOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	boundaries := ctx.Boundaries
	s := ctx.Scope

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
				LedgerName: ledger,
				Account:    target.Account.GetAddr(),
			},
			Key: order.GetKey(),
		}

		// Existence check (METADATA_NOT_FOUND on miss) — the stored value
		// itself is no longer captured into the log; the indexer resolves
		// the old encoded value via the reverse map on apply.
		if _, err := s.AccountMetadata().Get(metaKey); err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrMetadataNotFound{
					Target: target.Account.GetAddr(),
					Key:    order.GetKey(),
				}
			}

			return nil, &domain.ErrStorageOperation{Operation: "checking account metadata", Cause: err}
		}

		s.AccountMetadata().Delete(metaKey)
	case *commonpb.Target_TransactionId:
		txID := target.TransactionId
		if resolveErr := validateTransactionTarget(txID, boundaries); resolveErr != nil {
			return nil, resolveErr
		}

		txKey := domain.TransactionKey{LedgerName: ledger, ID: txID}

		stateReader, err := s.TransactionStates().Get(txKey)
		if errors.Is(err, domain.ErrNotFound) {
			return nil, &domain.ErrTransactionNotFound{TransactionID: txID}
		}

		if err != nil {
			return nil, &domain.ErrStorageOperation{Operation: "getting transaction state for delete", Cause: err}
		}

		state := stateReader.Mutate()

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
		s.TransactionStates().Put(txKey, state)
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
