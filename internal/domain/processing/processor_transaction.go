package processing

import (
	"errors"
	"fmt"
	"maps"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateTransaction(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.CreateTransactionOrder, s InMemoryStore, info *commonpb.LedgerInfo) (*commonpb.LedgerLogPayload, error) {
	// Check transaction reference uniqueness if reference is provided
	if order.GetReference() != "" {
		refKey := domain.TransactionReferenceKey{Ledger: ledger, Reference: order.GetReference()}

		existingRef, err := s.GetTransactionReference(refKey)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("checking transaction reference: %w", err)
		}

		if existingRef != nil {
			return nil, &domain.ErrTransactionReferenceConflict{
				Ledger:    ledger,
				Reference: order.GetReference(),
			}
		}
	}

	var schema *commonpb.MetadataSchema
	if info != nil {
		schema = info.GetMetadataSchema()
	}

	// Resolve script reference: load content from preloaded cache.
	if ref := order.GetNumscriptReference(); ref != nil {
		info, err := s.ResolveNumscriptContent(ledger, ref.GetName(), ref.GetVersion())
		if err != nil {
			return nil, fmt.Errorf("resolving numscript %q v%s: %w", ref.GetName(), ref.GetVersion(), err)
		}

		if info == nil {
			return nil, &domain.ErrNumscriptNotFound{Name: ref.GetName()}
		}

		order.Script = &commonpb.Script{
			Plain: info.GetContent(),
			Vars:  ref.GetVars(),
		}
	}

	// Select the appropriate posting producer
	var producer postingProducer
	isNumscript := order.GetScript() != nil && order.GetScript().GetPlain() != ""
	if isNumscript {
		producer = &numscriptPostingProducer{cache: p.numscriptCache, ledger: ledger, schema: schema}
	} else {
		producer = &stdPostingProducer{}
	}

	// Produce postings (handles balance checks and buffer updates)
	result, err := producer.produce(s, ledger, order)
	if err != nil {
		return nil, err
	}

	nextTransactionID := boundaries.GetNextTransactionId()
	boundaries.NextTransactionId = nextTransactionID + 1
	boundaries.PostingCount += uint64(len(result.Postings))

	if isNumscript {
		boundaries.NumscriptExecutionCount++
	}

	txKey := domain.TransactionKey{Ledger: ledger, ID: nextTransactionID}
	txState := &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
	}

	// Validate postings against account types.
	if compiled := p.getCompiledTypes(ledger, info); len(compiled) > 0 {
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
		enforceSchemaMap(schema, commonpb.TargetType_TARGET_TYPE_TRANSACTION, finalMetadata)
		txState.Metadata = finalMetadata
	}

	s.PutTransactionState(txKey, txState)

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

	// Enforce schema, capture previous values, and write to buffer.
	var previousAccountMetadata map[string]*commonpb.MetadataMap

	for account, mm := range accountMetadata {
		enforceSchemaMap(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, mm.GetValues())

		for key, value := range mm.GetValues() {
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{Ledger: ledger, Account: account},
				Key:        key,
			}

			// Capture old value before overwriting (for log replay in indexbuilder).
			if oldVal, err := s.GetAccountMetadata(metaKey); err == nil {
				if previousAccountMetadata == nil {
					previousAccountMetadata = make(map[string]*commonpb.MetadataMap)
				}

				prevMap := previousAccountMetadata[account]
				if prevMap == nil {
					prevMap = &commonpb.MetadataMap{Values: make(map[string]*commonpb.MetadataValue)}
					previousAccountMetadata[account] = prevMap
				}

				prevMap.Values[key] = oldVal
			}

			s.PutAccountMetadata(metaKey, value)
		}
	}

	// Store transaction reference if provided
	if order.GetReference() != "" {
		s.PutTransactionReference(
			domain.TransactionReferenceKey{Ledger: ledger, Reference: order.GetReference()},
			&commonpb.TransactionReferenceValue{TransactionId: nextTransactionID},
		)
	}

	// Use the user-provided timestamp, or fall back to the command date
	timestamp := order.GetTimestamp()
	if timestamp == nil {
		timestamp = s.GetDate()
	}

	// Compute post-commit volumes if requested
	var postCommitVolumes *commonpb.PostCommitVolumes
	if order.GetExpandVolumes() {
		postCommitVolumes = buildPostCommitVolumes(s, ledger, result.Postings)
	}

	// Get the current open period ID for the receipt
	var periodID uint64
	if p, ok := s.GetCurrentOpenPeriod(); ok {
		periodID = p.GetId()
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
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
				AccountMetadata:         accountMetadata,
				PeriodId:                periodID,
				PostCommitVolumes:       postCommitVolumes,
				PreviousAccountMetadata: previousAccountMetadata,
			},
		},
	}, nil
}

// produceResult holds the result of producing postings from an order.
// It includes the postings and any metadata set by the script.
type produceResult struct {
	Postings            []*commonpb.Posting
	TransactionMetadata map[string]*commonpb.MetadataValue            // Metadata from set_tx_meta in Numscript
	AccountsMetadata    map[string]map[string]*commonpb.MetadataValue // Metadata from set_account_meta in Numscript
}

type postingProducer interface {
	produce(s InMemoryStore, ledger string, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error)
}

type stdPostingProducer struct{}

func (p *stdPostingProducer) produce(s InMemoryStore, ledger string, order *raftcmdpb.CreateTransactionOrder) (*produceResult, error) {
	for _, posting := range order.GetPostings() {
		// Skip balance check when Force is true
		err := applyPosting(s, ledger, posting, order.GetForce())
		if err != nil {
			return nil, err
		}
	}

	return &produceResult{
		Postings:            order.GetPostings(),
		TransactionMetadata: nil, // No script metadata for standard postings
	}, nil
}
