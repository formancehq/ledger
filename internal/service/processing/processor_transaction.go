package processing

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/processing/numscript"
)

func (p *RequestProcessor) processCreateTransaction(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.CreateTransactionOrder, s InMemoryStore) (*commonpb.LedgerLogPayload, error) {
	// Check transaction reference uniqueness if reference is provided
	if order.Reference != "" {
		refKey := domain.TransactionReferenceKey{Ledger: ledger, Reference: order.Reference}
		existingRef, err := s.GetTransactionReference(refKey)
		if err != nil && !errors.Is(err, domain.ErrNotFound) {
			return nil, fmt.Errorf("checking transaction reference: %w", err)
		}
		if existingRef != nil {
			return nil, &domain.ErrTransactionReferenceConflict{
				Ledger:    ledger,
				Reference: order.Reference,
			}
		}
	}

	// Select the appropriate posting producer
	var producer postingProducer
	if order.Script != nil && order.Script.Plain != "" {
		producer = &numscriptPostingProducer{cache: p.numscriptCache, featureFlags: numscript.FeatureFlags, ledger: ledger}
	} else {
		producer = &stdPostingProducer{}
	}

	// Produce postings (handles balance checks and buffer updates)
	result, err := producer.produce(s, ledger, order)
	if err != nil {
		return nil, err
	}

	nextTransactionID := boundaries.NextTransactionId
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

	// Merge metadata: order metadata takes precedence over script metadata.
	// Uses typed []*Metadata directly to avoid map[string]string roundtrip.
	finalMetadata := order.Metadata
	if len(result.TransactionMetadata) > 0 {
		// Build a set of existing order keys for precedence check
		orderKeys := make(map[string]struct{})
		if finalMetadata != nil {
			for _, md := range finalMetadata.Metadata {
				orderKeys[md.Key] = struct{}{}
			}
		}
		// Append script metadata (order metadata takes precedence if key exists)
		merged := make([]*commonpb.Metadata, 0, len(orderKeys)+len(result.TransactionMetadata))
		if finalMetadata != nil {
			merged = append(merged, finalMetadata.Metadata...)
		}
		for _, md := range result.TransactionMetadata {
			if _, exists := orderKeys[md.Key]; !exists {
				merged = append(merged, md)
			}
		}
		finalMetadata = &commonpb.MetadataSet{Metadata: merged}
	}

	// Enforce schema on transaction and account metadata.
	var schema *commonpb.MetadataSchema
	if info, ok := s.GetLedger(ledger); ok {
		schema = info.MetadataSchema
	}
	if finalMetadata != nil {
		enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_TRANSACTION, finalMetadata.Metadata)
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
		enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, ms.Metadata)
		// Update buffer with schema-enforced values (numscript wrote string values)
		for _, md := range ms.Metadata {
			s.PutAccountMetadata(domain.MetadataKey{
				AccountKey: domain.AccountKey{Ledger: ledger, Account: account},
				Key:        md.Key,
			}, md.Value)
		}
	}
	if order.AccountMetadata != nil {
		for _, ms := range order.AccountMetadata {
			enforceSchema(schema, commonpb.TargetType_TARGET_TYPE_ACCOUNT, ms.Metadata)
		}
	}

	// Store transaction reference if provided
	if order.Reference != "" {
		s.PutTransactionReference(
			domain.TransactionReferenceKey{Ledger: ledger, Reference: order.Reference},
			&commonpb.TransactionReferenceValue{TransactionId: nextTransactionID},
		)
	}

	// Use the user-provided timestamp, or fall back to the command date
	timestamp := order.Timestamp
	if timestamp == nil {
		timestamp = s.GetDate()
	}

	// Get the current open period ID for the receipt
	var periodID uint64
	if p, ok := s.GetCurrentOpenPeriod(); ok {
		periodID = p.Id
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Postings:   result.Postings,
					Metadata:   finalMetadata,
					Timestamp:  timestamp,
					Reference:  order.Reference,
					Id:         nextTransactionID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
				AccountMetadata: accountMetadata,
				PeriodId:        periodID,
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
	for _, posting := range order.Postings {
		// Skip balance check when Force is true
		if err := applyPosting(s, ledger, posting, order.Force); err != nil {
			return nil, err
		}
	}

	return &produceResult{
		Postings:            order.Postings,
		TransactionMetadata: nil, // No script metadata for standard postings
	}, nil
}
