package processing

import (
	"errors"
	"fmt"
	"maps"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/domain"
	commonpb "github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func processCreateTransaction(ledger string, order *raftcmdpb.CreateTransactionOrder, ctx *Context) (*ledgerpb.LedgerLogPayload, domain.SerializableError) {
	if len(order.GetReference()) > domain.MaxTransactionReferenceLength {
		return nil, domain.ErrTransactionReferenceTooLong
	}
	boundaries := ctx.Boundaries
	s := ctx.Scope

	// Check transaction reference uniqueness if reference is provided.
	// This block is the strict prologue of the processor — no Scope.Put*
	// runs before it. That dry prologue is what makes it safe for
	// ProcessOrders to convert ErrTransactionReferenceConflict into an
	// OrderSkippedLog when the caller opted in via Order.skippable_reasons
	// (see processor_skippable.go for the per-reason audit log).
	if order.GetReference() != "" {
		refKey := domain.TransactionReferenceKey{LedgerName: ledger, Reference: order.GetReference()}

		existingRef, err := s.TransactionReferences().Get(refKey)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, domain.StoreFailure("checking transaction reference", err)
		}

		if existingRef != nil {
			return nil, &domain.ErrTransactionReferenceConflict{
				Ledger:                ledger,
				Reference:             order.GetReference(),
				ExistingTransactionID: existingRef.GetTransactionId(),
			}
		}
	}

	// Resolve script reference: load content from the preloaded cache. The
	// audited order keeps its selector ("latest" or an exact semver); "latest"
	// is resolved here, at apply time, to the greatest stored semver.
	var script *ledgerpb.Script
	if ref := order.GetNumscriptReference(); ref != nil {
		name := ref.GetName()
		version := ref.GetVersion()

		if version == "latest" {
			greatest, gErr := s.GetNumscriptLatestVersion(ledger, name)
			if gErr != nil {
				return nil, domain.StoreFailure(fmt.Sprintf("resolving latest numscript %q", name), gErr)
			}

			if greatest == "" {
				return nil, &domain.ErrNumscriptNotFound{Name: name}
			}

			// Admission preloaded the content for the version it observed as the
			// greatest. If the greatest advanced since planning (a concurrent
			// save committed first), that content key is not covered — reject as
			// stale so the retry re-plans against the new greatest.
			if s.CheckCoverage(dal.SubAttrNumscriptContent, domain.NumscriptEntryKey{LedgerName: ledger, Name: name, Version: greatest}) != nil {
				return nil, domain.ErrStaleProposal
			}

			version = greatest
		}

		info, err := s.ResolveNumscriptContent(ledger, name, version)
		if err != nil {
			return nil, domain.StoreFailure(fmt.Sprintf("resolving numscript %q v%s", name, version), err)
		}

		if info == nil {
			return nil, &domain.ErrNumscriptNotFound{Name: name, Version: version}
		}

		script = &ledgerpb.Script{
			Plain: info.GetContent(),
			Vars:  ref.GetVars(),
		}
	} else {
		script = order.GetScript()
	}

	isNumscript := script != nil && script.GetPlain() != ""

	// The persisted boundary stores the next allocatable ID. MaxUint64 itself
	// is therefore not allocatable: advancing it would wrap the boundary to zero
	// and make existing transaction keys reusable (EN-1860). For ordinary
	// transaction candidates, check before the posting producer stages any
	// volume mutation. Script candidates with no declared postings defer this
	// check until the producer confirms whether the script is metadata-only;
	// the proposal scope rolls back staged mutations if it turns out to be a
	// transaction and the sequence is exhausted.
	metadataOnlyCandidate := isNumscript && len(postings) == 0
	var nextTransactionID, advancedTransactionID uint64
	if !metadataOnlyCandidate {
		nextTransactionID = boundaries.GetNextTransactionId()
		advanced, exhausted := domain.CheckedNextSequence(nextTransactionID, domain.SequenceCounterTransactionID)
		if exhausted != nil {
			return nil, exhausted
		}
		advancedTransactionID = advanced
	}

	// Produce postings (handles balance checks and buffer updates)
	var result *produceResult
	var err domain.Describable
	if isNumscript {
		producer := &numscriptPostingProducer{
			cache:                ctx.NumscriptCache,
			ledgerName:           ledger,
			assetCache:           ctx.AssetCache,
			inputsResolutionHash: ctx.InputsResolutionHash,
		}
		result, err = producer.produceExecution(s, ledger, force, script)
	} else {
		producer := &stdPostingProducer{assetCache: ctx.AssetCache}
		result, err = producer.producePostings(s, ledger, postings, force)
	}
	if err != nil {
		return nil, err
	}

	return &stagedLedgerExecution{result: result, nextTransactionID: nextTransactionID, advancedTransactionID: advancedTransactionID, metadataOnlyCandidate: metadataOnlyCandidate}, nil
}

func processProducedTransaction(ledger string, order ledgerExecutionOrder, ctx *Context, staged *stagedLedgerExecution) (*commonpb.LedgerLogPayload, domain.Describable) {
	result := staged.result
	boundaries := ctx.Boundaries
	s := ctx.Scope
	info := ctx.LedgerInfo
	nextTransactionID := staged.nextTransactionID
	advancedTransactionID := staged.advancedTransactionID
	if staged.metadataOnlyCandidate {
		nextTransactionID = boundaries.GetNextTransactionId()
		var exhausted *domain.ErrSequenceExhausted
		advancedTransactionID, exhausted = domain.CheckedNextSequence(nextTransactionID, domain.SequenceCounterTransactionID)
		if exhausted != nil {
			return nil, exhausted
		}
	}

	boundaries.NextTransactionId = advancedTransactionID

	// posting_count, numscript_execution_count, revert_count, reference_count
	// are no longer maintained on LedgerBoundaries — they are derived from the
	// audit chain by internal/application/usagebuilder and served by the
	// LedgerStats handler out of usagestore. See EN-1420.

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
		Postings:     result.Postings,
	}

	// Validate account addresses in resolved postings (covers Numscript-resolved addresses).
	if err := validatePostings(result.Postings); err != nil {
		return nil, err
	}

	// Validate postings against account types.
	if compiled := compiledTypesFor(ctx.CompiledTypes, ledger, info); len(compiled) > 0 {
		if typeErr := validatePostingsAgainstAccountTypes(result.Postings, compiled, info.GetDefaultEnforcementMode()); typeErr != nil {
			return nil, typeErr
		}
	}

	// Merge script metadata under caller metadata (caller wins collisions)
	// into a map independent of order.Metadata: the audit chain captures the
	// accepted order bytes, so Numscript output must never be written back
	// into the caller-owned order map.
	finalMetadata := order.GetMetadata()

	if len(result.TransactionMetadata) > 0 {
		merged := make(map[string]*ledgerpb.MetadataValue, len(finalMetadata)+len(result.TransactionMetadata))
		maps.Copy(merged, result.TransactionMetadata)
		maps.Copy(merged, finalMetadata)
		finalMetadata = merged
	}

	// Bound the MERGED map, not the caller's. Admission already checked what the
	// caller sent and the producer checked each Numscript key/value shape, but
	// neither sees the union: two individually-legal halves can push one
	// transaction past the entity ceiling. The ceilings come from the committed
	// cluster policy through the Scope — never from node-local configuration,
	// which would make this committed entry apply differently per node
	// (invariant #2).
	limits := ctx.metadataLimits()

	if metaErr := limits.ValidateMap(finalMetadata); metaErr != nil {
		return nil, metaErr
	}

	if len(finalMetadata) > 0 {
		// Stored values are immutable. Coercion to declared_type happens at read.
		txState.Metadata = finalMetadata
	}

	s.TransactionStates().Put(txKey, txState)

	// Merge account metadata from script output and order.
	// Order metadata takes precedence over script metadata (same key → order wins).
	var accountMetadata map[string]*ledgerpb.MetadataMap
	if len(result.AccountsMetadata) > 0 {
		accountMetadata = make(map[string]*ledgerpb.MetadataMap, len(result.AccountsMetadata))
		for account, mdMap := range result.AccountsMetadata {
			accountMetadata[account] = &ledgerpb.MetadataMap{Values: mdMap}
		}
	}

	for account, mm := range order.GetAccountMetadata() {
		if accountMetadata == nil {
			accountMetadata = make(map[string]*ledgerpb.MetadataMap)
		}

		existing := accountMetadata[account]
		if existing == nil {
			accountMetadata[account] = mm
		} else {
			// Order keys take precedence: merge order entries into existing.
			maps.Copy(existing.GetValues(), mm.GetValues())
		}
	}

	// Same reasoning as the transaction map above, per account. Checked in its
	// own pass — and reporting the lexicographically smallest offending account
	// — so the rejection does not depend on Go map iteration order: this runs
	// inside apply, where a per-node choice of error would diverge the audit
	// chain.
	if metaErr := validateMergedAccountMetadata(accountMetadata, limits); metaErr != nil {
		return nil, metaErr
	}

	// Admission can only count caller input. Replace this transaction's input
	// contribution with its merged output in the proposal-wide budget, retaining
	// every other order's input and the output of earlier scripts.
	inputBytes := transactionMetadataSize(order.GetMetadata(), order.GetAccountMetadata())
	if ctx.metadataBudget == nil {
		// Direct handler callers execute a single transaction.
		ctx.metadataBudget = &commandMetadataBudget{bytes: inputBytes}
	}
	total := ctx.metadataBudget.bytes + transactionMetadataSize(finalMetadata, accountMetadata) - inputBytes
	if metaErr := limits.ValidateCommandBytes(total); metaErr != nil {
		return nil, metaErr
	}

	// Select the address failure before writing any account metadata. Its typed
	// details enter the audit chain, so first-error map iteration would fork
	// replicas even though the proposal's business writes are rolled back.
	if err := domain.ValidateAccountMetadataAddresses(accountMetadata); err != nil {
		return nil, err
	}

	// Stored values are immutable; the FSM does not coerce on write and no
	// longer captures previous values into the log. The indexer resolves
	// the old encoded value via the reverse map on overwrite.
	for account, mm := range accountMetadata {
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

	// Post-commit volumes are part of every persisted transaction: compute
	// them unconditionally from the volume state after this transaction's
	// postings applied (before any proposal-level ephemeral purge).
	postCommitVolumes, pcvErr := buildPostCommitVolumes(s, ledger, result.Postings)
	if pcvErr != nil {
		return nil, pcvErr
	}

	ctx.metadataBudget.bytes = total

	return &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &ledgerpb.CreatedTransaction{
				Transaction: &ledgerpb.Transaction{
					Postings:          result.Postings,
					Metadata:          finalMetadata,
					Timestamp:         timestamp,
					Reference:         order.GetReference(),
					Id:                nextTransactionID,
					InsertedAt:        s.GetDate().Mutate(),
					UpdatedAt:         s.GetDate().Mutate(),
					PostCommitVolumes: postCommitVolumes,
				},
				AccountMetadata: accountMetadata,
			},
		},
	}, nil
}

// processMetadataOnly applies account metadata emitted by a Numscript without
// allocating a transaction ID or creating TransactionState. It deliberately
// shares the producer and validation path with normal transaction creation, so
// script execution, metadata limits, address validation, and proposal rollback
// retain the same semantics.
func processMetadataOnly(ledger string, order ledgerExecutionOrder, result *produceResult, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	if len(result.Postings) > 0 {
		return nil, domain.ErrPostingsAndScriptConflict
	}

	accountMetadata := mergeAccountMetadata(result.AccountsMetadata, order.GetAccountMetadata())
	if !hasAccountMetadataUpdates(accountMetadata) {
		return nil, domain.ErrEmptyTransaction
	}
	if order.GetReference() != "" {
		return nil, domain.ErrMetadataOnlyReference
	}

	limits := ctx.metadataLimits()
	if metaErr := validateMergedAccountMetadata(accountMetadata, limits); metaErr != nil {
		return nil, metaErr
	}
	inputBytes := transactionMetadataSize(order.GetMetadata(), order.GetAccountMetadata())
	if ctx.metadataBudget == nil {
		ctx.metadataBudget = &commandMetadataBudget{bytes: inputBytes}
	}
	total := ctx.metadataBudget.bytes + transactionMetadataSize(nil, accountMetadata) - inputBytes
	if metaErr := limits.ValidateCommandBytes(total); metaErr != nil {
		return nil, metaErr
	}
	if err := domain.ValidateAccountMetadataAddresses(accountMetadata); err != nil {
		return nil, err
	}
	for account, mm := range accountMetadata {
		for key, value := range mm.GetValues() {
			ctx.Scope.AccountMetadata().Put(domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: ledger, Account: account}, Key: key}, value)
		}
	}
	ctx.metadataBudget.bytes = total

	return &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_AccountMetadataUpdated{AccountMetadataUpdated: &commonpb.AccountMetadataUpdated{AccountMetadata: accountMetadata}}}, nil
}

// Caller supplied keys take precedence over keys emitted by the script.
func mergeAccountMetadata(script map[string]map[string]*commonpb.MetadataValue, caller map[string]*commonpb.MetadataMap) map[string]*commonpb.MetadataMap {
	if len(script) == 0 && len(caller) == 0 {
		return nil
	}
	merged := make(map[string]*commonpb.MetadataMap, len(script)+len(caller))
	for account, values := range script {
		merged[account] = &commonpb.MetadataMap{Values: values}
	}
	for account, values := range caller {
		if existing := merged[account]; existing != nil {
			maps.Copy(existing.GetValues(), values.GetValues())
		} else {
			merged[account] = values
		}
	}

	return merged
}

func hasAccountMetadataUpdates(accountMetadata map[string]*commonpb.MetadataMap) bool {
	for _, metadata := range accountMetadata {
		if metadata != nil && len(metadata.GetValues()) > 0 {
			return true
		}
	}

	return false
}

func hasScriptAccountMetadataUpdates(accountMetadata map[string]map[string]*commonpb.MetadataValue) bool {
	for _, metadata := range accountMetadata {
		if len(metadata) > 0 {
			return true
		}
	}

	return false
}

// validateMergedAccountMetadata checks every account's merged metadata map
// against the size contract and reports the lexicographically smallest
// offending account. Selecting the smallest — rather than whichever account
// iteration reaches first — is what makes the rejection deterministic: this runs
// inside FSM apply, so two nodes iterating the same map in different orders must
// still produce the identical error (invariant #2), and the failure is
// hash-bound into the audit chain.
func validateMergedAccountMetadata(
	accountMetadata map[string]*ledgerpb.MetadataMap,
	limits domain.MetadataLimits,
) domain.Describable {
	var (
		worstAccount string
		worstErr     domain.Describable
	)

	for account, mm := range accountMetadata {
		err := limits.ValidateMap(mm.GetValues())
		if err == nil {
			continue
		}

		if worstErr == nil || account < worstAccount {
			worstAccount, worstErr = account, err
		}
	}

	if worstErr == nil {
		return nil
	}
	audited, ok := worstErr.(domain.SerializableError)
	if !ok {
		return worstErr
	}

	return &domain.ErrAccountValidation{Account: worstAccount, Cause: audited}
}

// validatePostings checks that all account addresses and assets in the postings
// contain only allowed characters. This runs after Numscript resolution so it
// covers both explicit and script-resolved values.
func validatePostings(postings []*ledgerpb.Posting) domain.SerializableError {
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

		if err := domain.ValidateColor(p.GetColor()); err != nil {
			return err
		}
	}

	return nil
}

// produceResult holds the result of producing postings from an order.
// It includes the postings and any metadata set by the script.
type produceResult struct {
	Postings            []*ledgerpb.Posting
	TransactionMetadata map[string]*ledgerpb.MetadataValue            // Metadata from set_tx_meta in Numscript
	AccountsMetadata    map[string]map[string]*ledgerpb.MetadataValue // Metadata from set_account_meta in Numscript
}

type postingProducer interface {
	produce(s Scope, ledger string, order *raftcmdpb.CreateTransactionOrder, script *ledgerpb.Script) (*produceResult, domain.SerializableError)
}

type stdPostingProducer struct {
	assetCache map[string]cachedAssetPrecision
}

func (p *stdPostingProducer) produce(s Scope, ledger string, order *raftcmdpb.CreateTransactionOrder, _ *ledgerpb.Script) (*produceResult, domain.SerializableError) {
	for _, posting := range order.GetPostings() {
		// Skip balance check when Force is true
		err := applyPosting(s, ledger, posting, force, p.assetCache)
		if err != nil {
			return nil, err
		}
	}

	return &produceResult{
		Postings:            postings,
		TransactionMetadata: nil, // No script metadata for standard postings
	}, nil
}

// commandMetadataBudget starts with all caller-supplied metadata in a proposal.
// Successful transaction merges add only their generated contribution: caller
// values win collisions, so the merged map never removes caller bytes.
type commandMetadataBudget struct {
	bytes uint64
}

func transactionMetadataSize(metadata map[string]*ledgerpb.MetadataValue, accounts map[string]*ledgerpb.MetadataMap) uint64 {
	total := domain.MetadataMapSize(metadata)
	for _, mm := range accounts {
		total += domain.MetadataMapSize(mm.GetValues())
	}

	return total
}
