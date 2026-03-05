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

	// Store the transaction init update for later indexing
	s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: nextTransactionID}, &commonpb.TransactionUpdate{
		ByLog: s.GetNextSequenceID(), // Will be set correctly when committing
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
				TransactionInit: &commonpb.TransactionInit{},
			},
		}},
	})

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
	var warnings []*commonpb.ChartViolation

	if info != nil && len(info.GetAccountTypes()) > 0 {
		var typeErr error

		warnings, typeErr = validatePostingsAgainstAccountTypes(result.Postings, info.GetAccountTypes())
		if typeErr != nil {
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
	}

	// Convert account metadata to protobuf format (already typed from produceResult).
	var accountMetadata map[string]*commonpb.MetadataSet
	if len(result.AccountsMetadata) > 0 {
		accountMetadata = make(map[string]*commonpb.MetadataSet, len(result.AccountsMetadata))
		for account, mdList := range result.AccountsMetadata {
			accountMetadata[account] = &commonpb.MetadataSet{Metadata: mdList}
		}
	}

	// Enforce schema on account metadata from script/order.
	for account, ms := range accountMetadata {
		enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, ms.GetMetadata())
		// Update buffer with schema-enforced values (numscript wrote string values)
		for _, md := range ms.GetMetadata() {
			s.PutAccountMetadata(domain.MetadataKey{
				AccountKey: domain.AccountKey{Ledger: ledger, Account: account},
				Key:        md.GetKey(),
			}, md.GetValue())
		}
	}

	if order.AccountMetadata != nil {
		for _, ms := range order.GetAccountMetadata() {
			enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, ms.GetMetadata())
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
				AccountMetadata:   accountMetadata,
				PeriodId:          periodID,
				PostCommitVolumes: postCommitVolumes,
				Warnings:          warnings,
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
