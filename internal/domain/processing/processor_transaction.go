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
	accountMetadata, err = applyDefaultMetadataToNewAccounts(s, ledger, result.Postings, compiled, accountMetadata)
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

// markNewAccountAndMatchDefaults is the shared per-account core for every
// account-creation path (EN-1276): transaction postings and metadata-set alike.
// When `account` is touched for the FIRST TIME EVER (any asset) it records the
// account-existence marker and returns the matching account type's
// default_metadata for the caller to merge. The marker is a UNIVERSAL account
// signal — written on every creation path regardless of whether the ledger
// declares default-bearing types — so a pre-existing account is always protected
// from later backfill. Returns (nil, nil) for the world system account, for
// accounts already created before this order, and for accounts matching no
// default-bearing type (the marker is still written in the last case — markers
// track existence, not just defaults).
//
// The marker value is the log's HLC date (s.GetDate(), identical to the
// LedgerLog.date the envelope is stamped with), which both keeps the serialized
// marker non-empty (snapshotter/preload tombstone trap) and feeds the
// insertion-date projection (EN-1360). Replay/rebuild reconstruct the same value
// from LedgerLog.date, so the bytes are identical for the same applied index.
//
// Newness is authoritative here at apply: GetAccount returns ErrNotFound only
// when the account has never been seen. The PutAccount marker is written to the
// WriteSet so a later order in the same batch (and the next order) sees the
// account as existing. A non-ErrNotFound error (e.g. a coverage miss from a
// stale plan that failed to declare the key) is surfaced loudly, never skipped.
func markNewAccountAndMatchDefaults(
	s Scope,
	ledgerName string,
	account string,
	compiled []accounttype.CompiledType,
) (map[string]*commonpb.MetadataValue, domain.Describable) {
	created, err := markAccountExistence(s, ledgerName, account)
	if err != nil {
		return nil, err
	}

	if !created {
		// world, or an account already created before this order — not new,
		// so no default metadata is (re-)applied.
		return nil, nil
	}

	matched := accounttype.FindMatchingType(account, compiled)
	if matched == nil {
		return nil, nil
	}

	return matched.GetDefaultMetadata(), nil
}

// markAccountExistence writes the universal per-account existence marker the
// first time `account` is ever created (by any posting or metadata-set), stamped
// with the log's HLC date. It is the marker-only core shared by every
// account-creation path —
// transaction postings, metadata-set, and mirror ingest alike — so a
// pre-existing account always carries a marker and is never backfilled when
// defaults are added later, regardless of ledger mode. Returns true when it
// wrote a NEW marker (the account had never been seen), false for the world
// system account and for accounts already created before this order.
//
// The marker value is the log's HLC date (s.GetDate(), identical to the
// LedgerLog.date the envelope is stamped with), which both keeps the serialized
// marker non-empty (snapshotter/preload tombstone trap) and feeds the
// insertion-date projection (EN-1360). Replay/rebuild reconstruct the same value
// from LedgerLog.date, so the bytes are identical for the same applied index.
//
// Newness is authoritative here at apply: GetAccount returns ErrNotFound only
// when the account has never been seen. The PutAccount marker is written to the
// WriteSet so a later order in the same batch (and the next order) sees the
// account as existing. A non-ErrNotFound error (e.g. a coverage miss from a
// stale plan that failed to declare the key) is surfaced loudly, never skipped.
func markAccountExistence(s Scope, ledgerName, account string) (bool, domain.Describable) {
	// System accounts (world) are never marked.
	if account == "world" {
		return false, nil
	}

	key := domain.AccountKey{LedgerName: ledgerName, Account: account}

	existing, err := s.GetAccount(key)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return false, &domain.ErrStorageOperation{Operation: "loading account state", Cause: err}
	}

	if existing != nil {
		return false, nil
	}

	s.PutAccount(key, &commonpb.AccountState{InsertionDate: s.GetDate().Mutate()})

	return true, nil
}

// markMirrorTouchedAccounts writes the universal existence marker for every
// non-world account a mirror-ingested log touches (postings + account-metadata
// targets), WITHOUT deriving or applying default metadata. Mirror parity means
// replaying exactly what the source ledger committed — the source already merged
// any defaults into the account metadata it sent — but the existence marker must
// still be written so a pre-existing account is protected from backfill after
// the ledger is promoted to NORMAL mode (EN-1276). The set of marked accounts
// matches recordTouchedAccounts (replay) and applyDefaultMetadataToNewAccounts
// (NORMAL apply), so live apply, replay and rebuild all agree.
func markMirrorTouchedAccounts(
	s Scope,
	ledgerName string,
	postings []*commonpb.Posting,
	accountMetadata map[string]*commonpb.MetadataMap,
) domain.Describable {
	seen := make(map[string]struct{}, len(postings)*2+len(accountMetadata))

	mark := func(account string) domain.Describable {
		if _, dup := seen[account]; dup {
			return nil
		}

		seen[account] = struct{}{}

		_, err := markAccountExistence(s, ledgerName, account)

		return err
	}

	for _, posting := range postings {
		for _, account := range [2]string{posting.GetSource(), posting.GetDestination()} {
			if err := mark(account); err != nil {
				return err
			}
		}
	}

	for account := range accountMetadata {
		if err := mark(account); err != nil {
			return err
		}
	}

	return nil
}

// applyDefaultMetadataToNewAccounts records an existence marker for each
// non-system account this transaction touches for the FIRST TIME EVER (any
// asset) and merges its matching account type's default_metadata into
// accountMetadata for keys not already set explicitly.
//
// The existence marker is written on every account-creation path regardless of
// whether the ledger declares default_metadata: it is a universal account
// signal, so a pre-existing account always carries a marker and is never
// backfilled when defaults are added later. The defaults MERGE, by contrast, is
// naturally gated by FindMatchingType inside markNewAccountAndMatchDefaults —
// an account matching no default-bearing type pays only the marker write.
//
// Determinism: the set of accounts, the marker date (LedgerLog.date), and the
// set of default keys written are independent of map iteration order, so replay
// is identical across nodes.
func applyDefaultMetadataToNewAccounts(
	s Scope,
	ledgerName string,
	postings []*commonpb.Posting,
	compiled []accounttype.CompiledType,
	accountMetadata map[string]*commonpb.MetadataMap,
) (map[string]*commonpb.MetadataMap, domain.Describable) {
	seen := make(map[string]struct{}, len(postings)*2+len(accountMetadata))

	mark := func(account string) domain.Describable {
		if _, dup := seen[account]; dup {
			return nil
		}

		seen[account] = struct{}{}

		defaults, err := markNewAccountAndMatchDefaults(s, ledgerName, account, compiled)
		if err != nil {
			return err
		}

		accountMetadata = mergeAccountDefaults(accountMetadata, account, defaults)

		return nil
	}

	for _, posting := range postings {
		for _, account := range [2]string{posting.GetSource(), posting.GetDestination()} {
			if err := mark(account); err != nil {
				return accountMetadata, err
			}
		}
	}

	// Metadata-only accounts: an account can appear solely in accountMetadata
	// (e.g. a Numscript set_account_meta targeting an account with no posting).
	// It is still a first-time account creation, so mark it and merge its
	// defaults — otherwise a later posting would see ErrNotFound and re-apply
	// defaults late. Admission declares the matching account need so this read
	// is coverage-gated (invariants #6/#9). Iteration order is irrelevant: the
	// set of marked accounts and merged keys is order-independent.
	for account := range accountMetadata {
		if err := mark(account); err != nil {
			return accountMetadata, err
		}
	}

	return accountMetadata, nil
}

// mergeAccountDefaults merges `defaults` into accountMetadata[account] for keys
// not already set explicitly (explicit script/order metadata always wins). The
// existing MetadataMap may alias the order's own proto, so it is cloned before
// the first mutation (CloneVT discipline) — a map we freshly allocate here is
// already ours. Returns accountMetadata (allocating it if defaults must be
// written into a previously-nil map). A nil/empty `defaults` is a no-op.
func mergeAccountDefaults(
	accountMetadata map[string]*commonpb.MetadataMap,
	account string,
	defaults map[string]*commonpb.MetadataValue,
) map[string]*commonpb.MetadataMap {
	if len(defaults) == 0 {
		return accountMetadata
	}

	mm := accountMetadata[account]
	owned := mm == nil

	for defKey, defValue := range defaults {
		if _, set := mm.GetValues()[defKey]; set {
			continue
		}

		switch {
		case mm == nil:
			mm = &commonpb.MetadataMap{Values: make(map[string]*commonpb.MetadataValue)}

			if accountMetadata == nil {
				accountMetadata = make(map[string]*commonpb.MetadataMap)
			}

			accountMetadata[account] = mm
			owned = true
		case !owned:
			mm = mm.CloneVT()

			if mm.Values == nil {
				mm.Values = make(map[string]*commonpb.MetadataValue)
			}

			accountMetadata[account] = mm
			owned = true
		}

		mm.Values[defKey] = defValue
	}

	return accountMetadata
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
