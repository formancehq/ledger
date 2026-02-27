package processing

import (
	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
)

// processMirrorIngest processes a single MirrorIngestOrder.
// It handles one v2 log entry: fill gaps, create transactions, save/delete metadata, reverts.
// The ledger must be in MIRROR mode.
func (p *RequestProcessor) processMirrorIngest(order *raftcmdpb.MirrorIngestOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	info, ok := s.GetLedger(order.Ledger)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.Ledger}
	}
	if info.Mode != commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.Ledger}
	}
	// Re-touch ledger info so it enters the Merge buffer and gets propagated
	// back to Gen0 on commit. Without this, ledger info is evicted after two
	// cache rotations because mirror proposals bypass the admission preloader.
	s.PutLedger(order.Ledger, info)

	boundaries, ok := s.GetBoundaries(order.Ledger)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.Ledger}
	}

	entry := order.Entry
	if entry == nil {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.Ledger}
	}

	var logPayload *commonpb.LedgerLogPayload

	switch data := entry.Data.(type) {
	case *raftcmdpb.MirrorLogEntry_FillGap:
		logPayload = p.processMirrorFillGap(order.Ledger, boundaries, data.FillGap, entry.V2LogId, s)

	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		logPayload = p.processMirrorCreatedTransaction(order.Ledger, boundaries, data.CreatedTransaction, s)

	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		logPayload = p.processMirrorSavedMetadata(order.Ledger, boundaries, data.SavedMetadata, s)

	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		logPayload = p.processMirrorDeletedMetadata(order.Ledger, boundaries, data.DeletedMetadata, s)

	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		logPayload = p.processMirrorRevertedTransaction(order.Ledger, boundaries, data.RevertedTransaction, s)

	default:
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.Ledger}
	}

	// Assign per-ledger log ID and advance boundaries
	nextLogID := boundaries.NextLogId
	boundaries.NextLogId = nextLogID + 1
	s.PutBoundaries(order.Ledger, boundaries)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: order.Ledger,
				Log: &commonpb.LedgerLog{
					Data: logPayload,
					Date: s.GetDate(),
					Id:   nextLogID,
				},
			},
		},
	}, nil
}

// processMirrorFillGap creates a FillGapLog for a v2 log that has no v3 equivalent.
// It also advances NextTransactionId for any skipped transaction IDs.
func (p *RequestProcessor) processMirrorFillGap(ledger string, boundaries *raftcmdpb.LedgerBoundaries, gap *raftcmdpb.MirrorFillGap, v2LogID uint64, s InMemoryStore) *commonpb.LedgerLogPayload {
	// Advance NextTransactionId for each skipped transaction
	for range gap.SkippedTransactionIds {
		boundaries.NextTransactionId++
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_FillGap{
			FillGap: &commonpb.FillGapLog{
				OriginalId: v2LogID,
			},
		},
	}
}

// processMirrorCreatedTransaction creates a transaction from mirror data.
// It applies postings with force=true (no balance checks) and assigns the exact transaction ID from v2.
func (p *RequestProcessor) processMirrorCreatedTransaction(ledger string, boundaries *raftcmdpb.LedgerBoundaries, ct *raftcmdpb.MirrorCreatedTransaction, s InMemoryStore) *commonpb.LedgerLogPayload {
	// Apply each posting with force=true (skip balance checks for mirror)
	for _, posting := range ct.Postings {
		// Ignore error — mirror force-applies all postings
		_ = applyPosting(s, ledger, posting, true)
	}

	txID := ct.TransactionId
	// Ensure NextTransactionId is past this ID
	if boundaries.NextTransactionId <= txID {
		boundaries.NextTransactionId = txID + 1
	}

	// Record transaction init
	s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: txID}, &commonpb.TransactionUpdate{
		ByLog: s.GetNextSequenceID(),
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
				TransactionInit: &commonpb.TransactionInit{},
			},
		}},
	})

	// Store reference if provided
	if ct.Reference != "" {
		s.PutTransactionReference(
			domain.TransactionReferenceKey{Ledger: ledger, Reference: ct.Reference},
			&commonpb.TransactionReferenceValue{TransactionId: txID},
		)
	}

	// Store account metadata
	var accountMetadata map[string]*commonpb.MetadataSet
	if len(ct.AccountMetadata) > 0 {
		accountMetadata = ct.AccountMetadata
		for account, ms := range ct.AccountMetadata {
			for _, md := range ms.Metadata {
				s.PutAccountMetadata(domain.MetadataKey{
					AccountKey: domain.AccountKey{Ledger: ledger, Account: account},
					Key:        md.Key,
				}, md.Value)
			}
		}
	}

	timestamp := ct.Timestamp
	if timestamp == nil {
		timestamp = s.GetDate()
	}

	var periodID uint64
	if p, ok := s.GetCurrentOpenPeriod(); ok {
		periodID = p.Id
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Postings:   ct.Postings,
					Metadata:   ct.Metadata,
					Timestamp:  timestamp,
					Reference:  ct.Reference,
					Id:         txID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
				AccountMetadata: accountMetadata,
				PeriodId:        periodID,
			},
		},
	}
}

// processMirrorSavedMetadata applies metadata from a v2 SET_METADATA log.
func (p *RequestProcessor) processMirrorSavedMetadata(ledger string, _ *raftcmdpb.LedgerBoundaries, sm *raftcmdpb.MirrorSavedMetadata, s InMemoryStore) *commonpb.LedgerLogPayload {
	if sm.Target != nil {
		switch target := sm.Target.Target.(type) {
		case *commonpb.Target_Account:
			if sm.Metadata != nil {
				for _, entry := range sm.Metadata.Metadata {
					s.PutAccountMetadata(domain.MetadataKey{
						AccountKey: domain.AccountKey{Ledger: ledger, Account: target.Account.Addr},
						Key:        entry.Key,
					}, entry.Value)
				}
			}
		case *commonpb.Target_Transaction:
			if sm.Metadata != nil {
				updates := make([]*commonpb.TransactionUpdateType, len(sm.Metadata.Metadata))
				for i, metadatum := range sm.Metadata.Metadata {
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
		}
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{
				Target:   sm.Target,
				Metadata: sm.Metadata,
			},
		},
	}
}

// processMirrorDeletedMetadata applies metadata deletion from a v2 DELETE_METADATA log.
func (p *RequestProcessor) processMirrorDeletedMetadata(ledger string, _ *raftcmdpb.LedgerBoundaries, dm *raftcmdpb.MirrorDeletedMetadata, s InMemoryStore) *commonpb.LedgerLogPayload {
	if dm.Target != nil {
		switch target := dm.Target.Target.(type) {
		case *commonpb.Target_Account:
			s.DeleteAccountMetadata(domain.MetadataKey{
				AccountKey: domain.AccountKey{Ledger: ledger, Account: target.Account.Addr},
				Key:        dm.Key,
			})
		case *commonpb.Target_Transaction:
			s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: target.Transaction.Id}, &commonpb.TransactionUpdate{
				ByLog: s.GetNextSequenceID(),
				Updates: []*commonpb.TransactionUpdateType{{
					TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationDeleteMetadata{
						TransactionModificationDeleteMetadata: &commonpb.TransactionUpdateDeleteMetadata{
							Key: dm.Key,
						},
					},
				}},
			})
		}
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{
				Target: dm.Target,
				Key:    dm.Key,
			},
		},
	}
}

// processMirrorRevertedTransaction processes a v2 REVERTED_TRANSACTION log.
func (p *RequestProcessor) processMirrorRevertedTransaction(ledger string, boundaries *raftcmdpb.LedgerBoundaries, rt *raftcmdpb.MirrorRevertedTransaction, s InMemoryStore) *commonpb.LedgerLogPayload {
	// Apply reversed postings with force=true
	for _, posting := range rt.ReversePostings {
		_ = applyPosting(s, ledger, posting, true)
	}

	// Mark original transaction as reverted
	s.PutReverted(domain.TransactionKey{Ledger: ledger, ID: rt.RevertedTransactionId}, true)

	revertTxID := rt.NewTransactionId
	// Ensure NextTransactionId is past this ID
	if boundaries.NextTransactionId <= revertTxID {
		boundaries.NextTransactionId = revertTxID + 1
	}

	// Add transaction update for the original transaction
	s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: rt.RevertedTransactionId}, &commonpb.TransactionUpdate{
		ByLog: s.GetNextSequenceID(),
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionModificationRevert{
				TransactionModificationRevert: &commonpb.TransactionUpdateRevert{
					ByTransaction: revertTxID,
				},
			},
		}},
	})

	// Add transaction init for the revert transaction
	s.AddTransactionUpdate(domain.TransactionKey{Ledger: ledger, ID: revertTxID}, &commonpb.TransactionUpdate{
		ByLog: s.GetNextSequenceID(),
		Updates: []*commonpb.TransactionUpdateType{{
			TransactionModificationTypePayload: &commonpb.TransactionUpdateType_TransactionInit{
				TransactionInit: &commonpb.TransactionInit{},
			},
		}},
	})

	timestamp := rt.Timestamp
	if timestamp == nil {
		timestamp = s.GetDate()
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: rt.RevertedTransactionId,
				RevertTransaction: &commonpb.Transaction{
					Postings:   rt.ReversePostings,
					Metadata:   rt.Metadata,
					Timestamp:  timestamp,
					Id:         revertTxID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
			},
		},
	}
}

// processPromoteLedger promotes a mirror ledger to normal mode.
func (p *RequestProcessor) processPromoteLedger(order *raftcmdpb.PromoteLedgerOrder, s InMemoryStore) (*commonpb.LogPayload, error) {
	info, ok := s.GetLedger(order.Ledger)
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.Ledger}
	}
	if info.Mode != commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.Ledger}
	}

	info.Mode = commonpb.LedgerMode_LEDGER_MODE_NORMAL
	info.MirrorSource = nil
	s.PutLedger(order.Ledger, info)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_PromoteLedger{
			PromoteLedger: &commonpb.PromoteLedgerLog{
				Info: info,
			},
		},
	}, nil
}
