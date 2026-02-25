package processing

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddMetadata(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.SaveMetadataOrder, s InMemoryStore) (*commonpb.LedgerLogPayload, error) {
	if order.Target == nil {
		return nil, domain.ErrTargetRequired
	}

	// Enforce schema: convert metadata values to declared types.
	if order.Metadata != nil {
		if info, ok := s.GetLedger(ledger); ok && info.MetadataSchema != nil {
			targetType := commonpb.TargetType_TARGET_TYPE_ACCOUNT
			if _, isTx := order.Target.Target.(*commonpb.Target_Transaction); isTx {
				targetType = commonpb.TargetType_TARGET_TYPE_TRANSACTION
			}
			enforceSchema(info.MetadataSchema, targetType, order.Metadata.Metadata)
		}
	}

	switch target := order.Target.Target.(type) {
	case *commonpb.Target_Account:
		for _, entry := range order.Metadata.Metadata {
			s.PutAccountMetadata(domain.MetadataKey{
				AccountKey: domain.AccountKey{
					Ledger:  ledger,
					Account: target.Account.Addr,
				},
				Key: entry.Key,
			}, entry.Value)
		}
	case *commonpb.Target_Transaction:
		if target.Transaction.Id >= boundaries.NextTransactionId {
			return nil, &domain.ErrTransactionNotFound{TransactionID: target.Transaction.Id}
		}
		// Group all metadata updates into a single TransactionUpdate
		// to avoid key collisions in PebbleDB (all updates in same request share the same ByLog)
		updates := make([]*commonpb.TransactionUpdateType, len(order.Metadata.Metadata))
		for i, metadatum := range order.Metadata.Metadata {
			updates[i] = &commonpb.TransactionUpdateType{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationAddMetadata{
					TransactionModificationAddMetadata: &commonpb.TransactionUpdateAddMetadata{
						Metadata: metadatum,
					},
				},
			}
		}
		s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: target.Transaction.Id}, &commonpb.TransactionUpdate{
			ByLog:   s.GetNextSequenceID(),
			Updates: updates,
		})
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{
				Target:   order.Target,
				Metadata: order.Metadata,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteMetadata(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.DeleteMetadataOrder, s InMemoryStore) (*commonpb.LedgerLogPayload, error) {
	if order.Target == nil {
		return nil, domain.ErrTargetRequired
	}
	if order.Key == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

	switch target := order.Target.Target.(type) {
	case *commonpb.Target_Account:
		metaKey := domain.MetadataKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledger,
				Account: target.Account.Addr,
			},
			Key: order.Key,
		}
		if _, err := s.GetAccountMetadata(metaKey); err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrMetadataNotFound{
					Target: target.Account.Addr,
					Key:    order.Key,
				}
			}
			return nil, fmt.Errorf("checking account metadata: %w", err)
		}
		s.DeleteAccountMetadata(metaKey)
	case *commonpb.Target_Transaction:
		if target.Transaction.Id >= boundaries.NextTransactionId {
			return nil, &domain.ErrTransactionNotFound{TransactionID: target.Transaction.Id}
		}
		// Use global sequence ID for ByLog (consistent with processCreateTransaction)
		// This ensures each transaction update has a unique key in PebbleDB
		s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: target.Transaction.Id}, &commonpb.TransactionUpdate{
			ByLog: s.GetNextSequenceID(),
			Updates: []*commonpb.TransactionUpdateType{{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationDeleteMetadata{
					TransactionModificationDeleteMetadata: &commonpb.TransactionUpdateDeleteMetadata{
						Key: order.Key,
					},
				},
			}},
		})
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{
				Target: order.Target,
				Key:    order.Key,
			},
		},
	}, nil
}
