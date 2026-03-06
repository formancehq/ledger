package processing

import (
	"errors"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

func (p *RequestProcessor) processAddMetadata(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.SaveMetadataOrder, s InMemoryStore) (*commonpb.LedgerLogPayload, error) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	// Load ledger info once for both chart validation and schema enforcement.
	var info *commonpb.LedgerInfo
	if ledgerInfo, ok := s.GetLedger(ledger); ok {
		info = ledgerInfo
	}

	// Validate account address against chart of accounts.
	var warnings []*commonpb.ChartViolation

	if acct, isAcct := order.GetTarget().GetTarget().(*commonpb.Target_Account); isAcct && info != nil && info.GetChartOfAccounts() != nil {
		var chartErr error

		warnings, chartErr = validateAccountInChartForAudit(acct.Account.GetAddr(), info.GetChartOfAccounts(), info.GetEnforcementMode())
		if chartErr != nil {
			return nil, chartErr
		}
	}

	// Enforce schema: convert metadata values to declared types.
	if order.GetMetadata() != nil && info != nil && info.GetMetadataSchema() != nil {
		targetType := commonpb.TargetType_TARGET_TYPE_ACCOUNT
		if _, isTx := order.GetTarget().GetTarget().(*commonpb.Target_Transaction); isTx {
			targetType = commonpb.TargetType_TARGET_TYPE_TRANSACTION
		}

		enforceSchema(info.GetMetadataSchema(), targetType, order.GetMetadata().GetMetadata())
	}

	switch target := order.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		for _, entry := range order.GetMetadata().GetMetadata() {
			s.PutAccountMetadata(domain.MetadataKey{
				AccountKey: domain.AccountKey{
					Ledger:  ledger,
					Account: target.Account.GetAddr(),
				},
				Key: entry.GetKey(),
			}, entry.GetValue())
		}
	case *commonpb.Target_Transaction:
		if target.Transaction.GetId() >= boundaries.GetNextTransactionId() {
			return nil, &domain.ErrTransactionNotFound{TransactionID: target.Transaction.GetId()}
		}
		// Group all metadata updates into a single TransactionUpdate
		// to avoid key collisions in PebbleDB (all updates in same request share the same ByLog)
		updates := make([]*commonpb.TransactionUpdateType, len(order.GetMetadata().GetMetadata()))
		for i, metadatum := range order.GetMetadata().GetMetadata() {
			updates[i] = &commonpb.TransactionUpdateType{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationAddMetadata{
					TransactionModificationAddMetadata: &commonpb.TransactionUpdateAddMetadata{
						Metadata: metadatum,
					},
				},
			}
		}

		s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}, &commonpb.TransactionUpdate{
			ByLog:   s.GetNextSequenceID(),
			Updates: updates,
		})
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{
				Target:   order.GetTarget(),
				Metadata: order.GetMetadata(),
				Warnings: warnings,
			},
		},
	}, nil
}

func (p *RequestProcessor) processDeleteMetadata(ledger string, boundaries *raftcmdpb.LedgerBoundaries, order *raftcmdpb.DeleteMetadataOrder, s InMemoryStore) (*commonpb.LedgerLogPayload, error) {
	if order.GetTarget() == nil {
		return nil, domain.ErrTargetRequired
	}

	if order.GetKey() == "" {
		return nil, domain.ErrMetadataKeyRequired
	}

	switch target := order.GetTarget().GetTarget().(type) {
	case *commonpb.Target_Account:
		metaKey := domain.MetadataKey{
			AccountKey: domain.AccountKey{
				Ledger:  ledger,
				Account: target.Account.GetAddr(),
			},
			Key: order.GetKey(),
		}
		if _, err := s.GetAccountMetadata(metaKey); err != nil {
			if errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrMetadataNotFound{
					Target: target.Account.GetAddr(),
					Key:    order.GetKey(),
				}
			}

			return nil, fmt.Errorf("checking account metadata: %w", err)
		}

		s.DeleteAccountMetadata(metaKey)
	case *commonpb.Target_Transaction:
		if target.Transaction.GetId() >= boundaries.GetNextTransactionId() {
			return nil, &domain.ErrTransactionNotFound{TransactionID: target.Transaction.GetId()}
		}
		// Use global sequence ID for ByLog (consistent with processCreateTransaction)
		// This ensures each transaction update has a unique key in PebbleDB
		s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: target.Transaction.GetId()}, &commonpb.TransactionUpdate{
			ByLog: s.GetNextSequenceID(),
			Updates: []*commonpb.TransactionUpdateType{{
				TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationDeleteMetadata{
					TransactionModificationDeleteMetadata: &commonpb.TransactionUpdateDeleteMetadata{
						Key: order.GetKey(),
					},
				},
			}},
		})
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{
				Target: order.GetTarget(),
				Key:    order.GetKey(),
			},
		},
	}, nil
}
