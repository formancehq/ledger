package processing

import (
	"errors"
	"fmt"
	"maps"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func processCreateTransaction(ledger string, order *raftcmdpb.CreateTransactionOrder, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	boundaries := ctx.Boundaries
	s := ctx.Scope
	info := ctx.LedgerInfo

	// Check transaction reference uniqueness if reference is provided
	if order.GetReference() != "" {
		refKey := domain.TransactionReferenceKey{LedgerName: ledger, Reference: order.GetReference()}

		existingRef, err := s.TransactionReferences().Get(refKey)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, &domain.ErrStorageOperation{Operation: "checking transaction reference", Cause: err}
		}

		if existingRef != nil {
			return nil, &domain.ErrTransactionReferenceConflict{
				Ledger:    ledger,
				Reference: order.GetReference(),
			}
		}
	}

	// Resolve script reference: load content from preloaded cache.
	var script *commonpb.Script
	if ref := order.GetNumscriptReference(); ref != nil {
		info, err := s.ResolveNumscriptContent(ledger, ref.GetName(), ref.GetVersion())
		if err != nil {
			return nil, &domain.ErrStorageOperation{
				Operation: fmt.Sprintf("resolving numscript %q v%s", ref.GetName(), ref.GetVersion()),
				Cause:     err,
			}
		}

		if info == nil {
			return nil, &domain.ErrNumscriptNotFound{Name: ref.GetName()}
		}

		script = &commonpb.Script{
			Plain: info.GetContent(),
			Vars:  ref.GetVars(),
		}
	} else {
		script = order.GetScript()
	}

	// Select the appropriate posting producer
	var producer postingProducer
	isNumscript := script != nil && script.GetPlain() != ""
	if isNumscript {
		producer = &numscriptPostingProducer{cache: ctx.NumscriptCache, ledgerName: ledger, assetCache: ctx.AssetCache}
	} else {
		producer = &stdPostingProducer{assetCache: ctx.AssetCache}
	}

	// Produce postings (handles balance checks and buffer updates)
	result, err := producer.produce(s, ledger, order, script)
	if err != nil {
		return nil, err
	}

	// Post-producer invariant: a created transaction must move at least one
	// posting. The structural admission gate (validateOrderContent) rejects
	// orders with no content source, but a numscript that runs cleanly yet
	// emits no `send` (or whose `send` short-circuits via vars) only surfaces
	// here. Without this guard the FSM commits a zero-posting log entry
	// (#452).
	if len(result.Postings) == 0 {
		return nil, domain.ErrEmptyTransaction
	}

	nextTransactionID := boundaries.GetNextTransactionId()
	boundaries.NextTransactionId = nextTransactionID + 1
	boundaries.PostingCount += uint64(len(result.Postings))

	if isNumscript {
		boundaries.NumscriptExecutionCount++
	}

	// Use the user-provided timestamp, or fall back to the command date.
	// The effective timestamp is recorded on TransactionState so reverts can
	// honor at_effective_date without re-reading the original log from Pebble.
	timestamp := order.GetTimestamp()
	if timestamp == nil {
		timestamp = s.GetDate().Mutate()
	}

	txKey := domain.TransactionKey{LedgerName: ledger, ID: nextTransactionID}
	txState := &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Timestamp:    timestamp,
	}

	// Validate account addresses in resolved postings (covers Numscript-resolved addresses).
	if err := validatePostings(result.Postings); err != nil {
		return nil, err
	}

	// Compile account types once: reused for posting validation and for
	// default-metadata application on newly-created accounts below.
	compiled := compiledTypesFor(ctx.CompiledTypes, ledger, info)

	// Validate postings against account types.
	if len(compiled) > 0 {
		if typeErr := validatePostingsAgainstAccountTypes(result.Postings, compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
			return nil, typeErr
		}
	}

	// Merge metadata: order metadata takes precedence over script metadata.
	finalMetadata := order.GetMetadata()

	if len(result.TransactionMetadata) > 0 {
		if finalMetadata == nil {
			finalMetadata = make(map[string]*commonpb.MetadataValue, len(result.TransactionMetadata))
		}

		// Append script metadata (order metadata takes precedence if key exists)
		for key, value := range result.TransactionMetadata {
			if _, exists := finalMetadata[key]; !exists {
				finalMetadata[key] = value
			}
		}
	}

	if len(finalMetadata) > 0 {
		// Stored values are immutable. Coercion to declared_type happens at read.
		txState.Metadata = finalMetadata
	}

	s.TransactionStates().Put(txKey, txState)

	// Merge account metadata from script output and order.
	// Order metadata takes precedence over script metadata (same key → order wins).
	var accountMetadata map[string]*commonpb.MetadataMap
	if len(result.AccountsMetadata) > 0 {
		accountMetadata = make(map[string]*commonpb.MetadataMap, len(result.AccountsMetadata))
		for account, mdMap := range result.AccountsMetadata {
			accountMetadata[account] = &commonpb.MetadataMap{Values: mdMap}
		}
	}

	for account, mm := range order.GetAccountMetadata() {
		if accountMetadata == nil {
			accountMetadata = make(map[string]*commonpb.MetadataMap)
		}

		existing := accountMetadata[account]
		if existing == nil {
			accountMetadata[account] = mm
		} else {
			// Order keys take precedence: merge order entries into existing.
			maps.Copy(existing.GetValues(), mm.GetValues())
		}
	}

	// Apply account-type default metadata to accounts created for the first
	// time by this transaction (EN-1276). Runs after the explicit
	// script/order metadata is merged so explicit keys win; merges into
	// accountMetadata so defaults ride the same PutAccountMetadata + log path.
	accountMetadata, err = applyDefaultMetadataToNewAccounts(s, ledger, info, result.Postings, compiled, accountMetadata)
	if err != nil {
		return nil, err
	}

	// Stored values are immutable; the FSM does not coerce on write and no
	// longer captures previous values into the log. The indexer resolves
	// the old encoded value via the reverse map on overwrite.
	for account, mm := range accountMetadata {
		if err := domain.ValidateAccountAddress(account); err != nil {
			return nil, err
		}

		for key, value := range mm.GetValues() {
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{LedgerName: ledger, Account: account},
				Key:        key,
			}

			s.AccountMetadata().Put(metaKey, value)
		}
	}

	// Store transaction reference if provided
	if order.GetReference() != "" {
		s.TransactionReferences().Put(
			domain.TransactionReferenceKey{LedgerName: ledger, Reference: order.GetReference()},
			&commonpb.TransactionReferenceValue{TransactionId: nextTransactionID},
		)
	}

	// Compute post-commit volumes if requested
	var postCommitVolumes *commonpb.PostCommitVolumes
	if order.GetExpandVolumes() {
		postCommitVolumes = buildPostCommitVolumes(s, ledger, result.Postings)
	}

	// Get the current open chapter ID for the receipt
	var chapterID uint64
	if p, ok := s.GetCurrentOpenChapter(); ok {
		chapterID = p.GetId()
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Postings:   result.Postings,
					Metadata:   finalMetadata,
					Timestamp:  timestamp,
					Reference:  order.GetReference(),
					Id:         nextTransactionID,
					InsertedAt: s.GetDate().Mutate(),
					UpdatedAt:  s.GetDate().Mutate(),
				},
				AccountMetadata:   accountMetadata,
				ChapterId:         chapterID,
				PostCommitVolumes: postCommitVolumes,
			},
		},
	}, nil
}

// ledgerHasAccountTypeDefaults reports whether any account type on the ledger
// declares default_metadata. Derived from the audit-built LedgerInfo, so it is
// deterministic across nodes and replay with no stored state — the apply-path
// gate for EN-1276 default-metadata application.
func ledgerHasAccountTypeDefaults(info *commonpb.LedgerInfo) bool {
	for _, at := range info.GetAccountTypes() {
		if len(at.GetDefaultMetadata()) > 0 {
			return true
		}
	}

	return false
}

// applyDefaultMetadataToNewAccounts records an existence marker for each
// non-system account this transaction touches for the FIRST TIME EVER (any
// asset) and merges its matching account type's default_metadata into
// accountMetadata for keys not already set explicitly. It is a no-op unless the
// ledger has activated the feature (AccountDefaultsStatus == READY), so ledgers
// that declare no default_metadata — and ledgers still seeding — pay nothing.
//
// Newness is authoritative here at apply: GetAccount returns ErrNotFound only
// when the account has never been seen. The PutAccount marker is written to the
// WriteSet so a later order in the same batch (and the next transaction) sees
// the account as existing — an account first seen in this batch is created
// once. A non-ErrNotFound error (e.g. a coverage miss from a stale plan that
// failed to declare the key) is surfaced loudly rather than silently skipped.
//
// Determinism: the set of accounts and the set of default keys written are
// independent of map iteration order, so replay is identical across nodes.
func applyDefaultMetadataToNewAccounts(
	s Scope,
	ledgerName string,
	info *commonpb.LedgerInfo,
	postings []*commonpb.Posting,
	compiled []accounttype.CompiledType,
	accountMetadata map[string]*commonpb.MetadataMap,
) (map[string]*commonpb.MetadataMap, domain.Describable) {
	// Apply-path gate (behaviour-first): derived deterministically from the
	// audit-built LedgerInfo, so it survives replay/recovery without any stored
	// status. Correct for newly-created ledgers (every account they ever see is
	// genuinely new). The stored AccountDefaultsStatus + one-time seeding — which
	// also lets default_metadata be added to a ledger that already has accounts
	// without backfilling them — replaces this derived gate in a later phase.
	if !ledgerHasAccountTypeDefaults(info) {
		return accountMetadata, nil
	}

	createdByLog := s.GetNextSequenceID()
	seen := make(map[string]struct{}, len(postings)*2)

	for _, posting := range postings {
		for _, account := range [2]string{posting.GetSource(), posting.GetDestination()} {
			// System accounts (world) are never assigned default metadata.
			if account == "world" {
				continue
			}

			if _, dup := seen[account]; dup {
				continue
			}

			seen[account] = struct{}{}

			key := domain.AccountKey{LedgerName: ledgerName, Account: account}

			existing, err := s.GetAccount(key)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return accountMetadata, &domain.ErrStorageOperation{Operation: "loading account state", Cause: err}
			}

			if existing != nil {
				// Account already created before this transaction — not new.
				continue
			}

			// First time this account is ever touched: record the marker so it
			// is recognised as existing from now on.
			s.PutAccount(key, &commonpb.AccountState{CreatedByLog: createdByLog})

			matched := accounttype.FindMatchingType(account, compiled)
			if matched == nil {
				continue
			}

			defaults := matched.GetDefaultMetadata()
			if len(defaults) == 0 {
				continue
			}

			mm := accountMetadata[account]

			for defKey, defValue := range defaults {
				// Explicit script/order metadata for the same key always wins.
				if mm != nil {
					if _, set := mm.GetValues()[defKey]; set {
						continue
					}
				}

				if mm == nil {
					mm = &commonpb.MetadataMap{Values: make(map[string]*commonpb.MetadataValue)}

					if accountMetadata == nil {
						accountMetadata = make(map[string]*commonpb.MetadataMap)
					}

					accountMetadata[account] = mm
				}

				if mm.Values == nil {
					mm.Values = make(map[string]*commonpb.MetadataValue)
				}

				mm.Values[defKey] = defValue
			}
		}
	}

	return accountMetadata, nil
}

// validatePostings checks that all account addresses and assets in the postings
// contain only allowed characters. This runs after Numscript resolution so it
// covers both explicit and script-resolved values.
func validatePostings(postings []*commonpb.Posting) domain.Describable {
	for _, p := range postings {
		if err := domain.ValidateAccountAddress(p.GetSource()); err != nil {
			return err
		}

		if err := domain.ValidateAccountAddress(p.GetDestination()); err != nil {
			return err
		}

		if err := domain.ValidateAsset(p.GetAsset()); err != nil {
			return err
		}
	}

	return nil
}

// produceResult holds the result of producing postings from an order.
// It includes the postings and any metadata set by the script.
type produceResult struct {
	Postings            []*commonpb.Posting
	TransactionMetadata map[string]*commonpb.MetadataValue            // Metadata from set_tx_meta in Numscript
	AccountsMetadata    map[string]map[string]*commonpb.MetadataValue // Metadata from set_account_meta in Numscript
}

type postingProducer interface {
	produce(s Scope, ledger string, order *raftcmdpb.CreateTransactionOrder, script *commonpb.Script) (*produceResult, domain.Describable)
}

type stdPostingProducer struct {
	assetCache map[string]cachedAssetPrecision
}

func (p *stdPostingProducer) produce(s Scope, ledger string, order *raftcmdpb.CreateTransactionOrder, _ *commonpb.Script) (*produceResult, domain.Describable) {
	for _, posting := range order.GetPostings() {
		// Skip balance check when Force is true
		err := applyPosting(s, ledger, posting, order.GetForce(), p.assetCache)
		if err != nil {
			return nil, err
		}
	}

	return &produceResult{
		Postings:            order.GetPostings(),
		TransactionMetadata: nil, // No script metadata for standard postings
	}, nil
}
