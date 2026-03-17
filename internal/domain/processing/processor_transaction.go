package processing

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processCreateTransaction(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.CreateTransactionOrder, s InMemoryStore) (*commonpb.LedgerLogPayload, error) {
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

	// Select the appropriate posting producer
	var producer postingProducer
	if order.GetScript() != nil && order.GetScript().GetPlain() != "" {
		producer = &numscriptPostingProducer{cache: p.numscriptCache, ledger: ledger}
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

	txKey := domain.TransactionKey{Ledger: ledger, ID: nextTransactionID}
	txState := &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
	}

	// Load ledger info once for chart validation and schema enforcement (DRY).
	var (
		schema *commonpb.MetadataSchema
		info   *commonpb.LedgerInfo
	)
	if ledgerInfo, ok := s.GetLedger(ledger); ok {
		info = ledgerInfo
		schema = ledgerInfo.GetMetadataSchema()
	}

	// Validate postings against account types.
	if info != nil && len(info.GetAccountTypes()) > 0 {
		if typeErr := validatePostingsAgainstAccountTypes(result.Postings, info.GetAccountTypes()); typeErr != nil {
			return nil, typeErr
		}
	}

	// Merge metadata: order metadata takes precedence over script metadata.
	// Uses typed []*Metadata directly to avoid map[string]string roundtrip.
	finalMetadata := order.GetMetadata()

	if len(result.TransactionMetadata) > 0 {
		// Build a set of existing order keys for precedence check
		orderKeys := make(map[string]struct{})

		if finalMetadata != nil {
			for _, md := range finalMetadata.GetMetadata() {
				orderKeys[md.GetKey()] = struct{}{}
			}
		}
		// Append script metadata (order metadata takes precedence if key exists)
		merged := make([]*commonpb.Metadata, 0, len(orderKeys)+len(result.TransactionMetadata))
		if finalMetadata != nil {
			merged = append(merged, finalMetadata.GetMetadata()...)
		}

		for _, md := range result.TransactionMetadata {
			if _, exists := orderKeys[md.GetKey()]; !exists {
				merged = append(merged, md)
			}
		}

		finalMetadata = &commonpb.MetadataSet{Metadata: merged}
	}

	if finalMetadata != nil {
		enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_TRANSACTION, finalMetadata.GetMetadata())
		txState.Metadata = finalMetadata
	}

	s.PutTransactionState(txKey, txState)

	// Merge account metadata from script output and order.
	// Order metadata takes precedence over script metadata (same key → order wins).
	var accountMetadata map[string]*commonpb.MetadataSet
	if len(result.AccountsMetadata) > 0 {
		accountMetadata = make(map[string]*commonpb.MetadataSet, len(result.AccountsMetadata))
		for account, mdList := range result.AccountsMetadata {
			accountMetadata[account] = &commonpb.MetadataSet{Metadata: mdList}
		}
	}

	for account, ms := range order.GetAccountMetadata() {
		if accountMetadata == nil {
			accountMetadata = make(map[string]*commonpb.MetadataSet)
		}

		existing := accountMetadata[account]
		if existing == nil {
			accountMetadata[account] = ms
		} else {
			// Order keys take precedence: build set of order keys, keep only
			// script entries whose key is not overridden.
			orderKeys := make(map[string]struct{}, len(ms.GetMetadata()))
			for _, md := range ms.GetMetadata() {
				orderKeys[md.GetKey()] = struct{}{}
			}

			merged := make([]*commonpb.Metadata, 0, len(existing.GetMetadata())+len(ms.GetMetadata()))
			for _, md := range existing.GetMetadata() {
				if _, overridden := orderKeys[md.GetKey()]; !overridden {
					merged = append(merged, md)
				}
			}

			merged = append(merged, ms.GetMetadata()...)
			accountMetadata[account] = &commonpb.MetadataSet{Metadata: merged}
		}
	}

	// Enforce schema, capture previous values, and write to buffer.
	var previousAccountMetadata map[string]*commonpb.MetadataSet

	for account, ms := range accountMetadata {
		enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, ms.GetMetadata())

		for _, md := range ms.GetMetadata() {
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{Ledger: ledger, Account: account},
				Key:        md.GetKey(),
			}

			// Capture old value before overwriting (for log replay in indexbuilder).
			if oldVal, err := s.GetAccountMetadata(metaKey); err == nil {
				if previousAccountMetadata == nil {
					previousAccountMetadata = make(map[string]*commonpb.MetadataSet)
				}

				prevSet := previousAccountMetadata[account]
				if prevSet == nil {
					prevSet = &commonpb.MetadataSet{}
					previousAccountMetadata[account] = prevSet
				}

				prevSet.Metadata = append(prevSet.Metadata, &commonpb.Metadata{
					Key:   md.GetKey(),
					Value: oldVal,
				})
			}

			s.PutAccountMetadata(metaKey, md.GetValue())
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
// Metadata is carried as typed []*commonpb.Metadata to avoid string roundtrips.
type produceResult struct {
	Postings            []*commonpb.Posting
	TransactionMetadata []*commonpb.Metadata            // Metadata from set_tx_meta in Numscript
	AccountsMetadata    map[string][]*commonpb.Metadata // Metadata from set_account_meta in Numscript
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
