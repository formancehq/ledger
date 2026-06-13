package processing

import (
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processMirrorIngest processes a single MirrorIngestOrder.
// It handles one v2 log entry: fill gaps, create transactions, save/delete metadata, reverts.
// The ledger must be in MIRROR mode.
func (p *RequestProcessor) processMirrorIngest(order *raftcmdpb.MirrorIngestOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	info, ok := s.GetLedger(order.GetLedger())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.GetLedger()}
	}

	if info.GetMode() != commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.GetLedger()}
	}
	// Re-touch ledger info so it enters the Merge buffer and gets propagated
	// back to Gen0 on commit. Without this, ledger info is evicted after two
	// cache rotations because mirror proposals bypass the admission preloader.
	s.PutLedger(order.GetLedger(), info)

	boundariesReader, ok := s.GetBoundaries(order.GetLedger())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.GetLedger()}
	}

	boundaries := boundariesReader.Mutate()

	entry := order.GetEntry()
	if entry == nil {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.GetLedger()}
	}

	ledgerID := info.GetId()

	var logPayload *commonpb.LedgerLogPayload

	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_FillGap:
		logPayload = p.processMirrorFillGap(order.GetLedger(), boundaries, data.FillGap, entry.GetV2LogId(), s)

	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		var err domain.Describable

		logPayload, err = p.processMirrorCreatedTransaction(order.GetLedger(), ledgerID, boundaries, data.CreatedTransaction, s)
		if err != nil {
			return nil, err
		}

	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		logPayload = p.processMirrorSavedMetadata(order.GetLedger(), ledgerID, boundaries, data.SavedMetadata, s)

	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		logPayload = p.processMirrorDeletedMetadata(order.GetLedger(), ledgerID, boundaries, data.DeletedMetadata, s)

	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		var err domain.Describable

		logPayload, err = p.processMirrorRevertedTransaction(order.GetLedger(), ledgerID, boundaries, data.RevertedTransaction, s)
		if err != nil {
			return nil, err
		}

	default:
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.GetLedger()}
	}

	// Assign per-ledger log ID and advance boundaries
	nextLogID := boundaries.GetNextLogId()
	boundaries.NextLogId = nextLogID + 1
	s.PutBoundaries(order.GetLedger(), boundaries)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: order.GetLedger(),
				Log: &commonpb.LedgerLog{
					Data: logPayload,
					Date: s.GetDate(),
					Id:   nextLogID,
				},
			},
		},
	}, nil
}

// processMirrorFillGap creates a FilledGapLog for a v2 log that has no v3 equivalent.
// It also advances NextTransactionId for any skipped transaction IDs.
func (p *RequestProcessor) processMirrorFillGap(ledger string, boundaries *raftcmdpb.LedgerBoundaries, gap *raftcmdpb.MirrorFillGap, v2LogID uint64, s InMemoryStore) *commonpb.LedgerLogPayload {
	// Advance NextTransactionId for each skipped transaction
	for range gap.GetSkippedTransactionIds() {
		boundaries.NextTransactionId++
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_FillGap{
			FillGap: &commonpb.FilledGapLog{
				OriginalId: v2LogID,
			},
		},
	}
}

// processMirrorCreatedTransaction creates a transaction from mirror data.
// It applies postings with force=true (no balance checks) and assigns the exact transaction ID from v2.
// Missing volumes are auto-initialized to zero so postings are never silently skipped.
func (p *RequestProcessor) processMirrorCreatedTransaction(ledger string, ledgerID uint32, boundaries *raftcmdpb.LedgerBoundaries, ct *raftcmdpb.MirrorCreatedTransaction, s InMemoryStore) (*commonpb.LedgerLogPayload, domain.Describable) {
	// Apply each posting with force=true (skip balance checks, auto-init missing volumes)
	for _, posting := range ct.GetPostings() {
		if err := applyPosting(s, ledgerID, posting, true, p.assetCache); err != nil {
			// applyPosting already returns a Describable (ErrBalanceNotPreloaded,
			// ErrInsufficientFunds, ErrVolumeOverflow); propagate verbatim.
			return nil, err
		}
	}

	txID := ct.GetTransactionId()
	// Ensure NextTransactionId is past this ID
	if boundaries.GetNextTransactionId() <= txID {
		boundaries.NextTransactionId = txID + 1
	}
	boundaries.PostingCount += uint64(len(ct.GetPostings()))

	// Record transaction state (include metadata from the mirrored transaction)
	s.PutTransactionState(domain.TransactionKey{LedgerID: ledgerID, ID: txID}, &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Metadata:     ct.GetMetadata(),
	})

	// Store reference if provided
	if ct.GetReference() != "" {
		s.PutTransactionReference(
			domain.TransactionReferenceKey{LedgerID: ledgerID, Reference: ct.GetReference()},
			&commonpb.TransactionReferenceValue{TransactionId: txID},
		)
	}

	// Store account metadata
	var (
		accountMetadata         map[string]*commonpb.MetadataMap
		previousAccountMetadata map[string]*commonpb.MetadataMap
	)

	if len(ct.GetAccountMetadata()) > 0 {
		accountMetadata = ct.GetAccountMetadata()
		for account, mm := range ct.GetAccountMetadata() {
			for key, value := range mm.GetValues() {
				metaKey := domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: account},
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
	}

	timestamp := ct.GetTimestamp()
	if timestamp == nil {
		timestamp = s.GetDate()
	}

	var periodID uint64
	if p, ok := s.GetCurrentOpenPeriod(); ok {
		periodID = p.GetId()
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{
			CreatedTransaction: &commonpb.CreatedTransaction{
				Transaction: &commonpb.Transaction{
					Postings:   ct.GetPostings(),
					Metadata:   ct.GetMetadata(),
					Timestamp:  timestamp,
					Reference:  ct.GetReference(),
					Id:         txID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
				AccountMetadata:         accountMetadata,
				PeriodId:                periodID,
				PreviousAccountMetadata: previousAccountMetadata,
			},
		},
	}, nil
}

// processMirrorSavedMetadata applies metadata from a v2 SET_METADATA log.
func (p *RequestProcessor) processMirrorSavedMetadata(ledger string, ledgerID uint32, _ *raftcmdpb.LedgerBoundaries, sm *raftcmdpb.MirrorSavedMetadata, s InMemoryStore) *commonpb.LedgerLogPayload {
	var previousValues map[string]*commonpb.MetadataValue

	if sm.GetTarget() != nil {
		switch target := sm.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			for key, value := range sm.GetMetadata() {
				metaKey := domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: target.Account.GetAddr()},
					Key:        key,
				}

				// Capture old value before overwriting.
				if oldVal, err := s.GetAccountMetadata(metaKey); err == nil {
					if previousValues == nil {
						previousValues = make(map[string]*commonpb.MetadataValue)
					}

					previousValues[key] = oldVal
				}

				s.PutAccountMetadata(metaKey, value)
			}
		case *commonpb.Target_Transaction:
			if len(sm.GetMetadata()) > 0 {
				txKey := domain.TransactionKey{LedgerID: ledgerID, ID: target.Transaction.GetId()}

				state, _ := s.GetTransactionState(txKey)
				if state != nil {
					state = state.CloneVT()

					if state.GetMetadata() == nil {
						state.Metadata = make(map[string]*commonpb.MetadataValue)
					}

					for key, value := range sm.GetMetadata() {
						// Capture old value before overwriting.
						if existing, ok := state.GetMetadata()[key]; ok {
							if previousValues == nil {
								previousValues = make(map[string]*commonpb.MetadataValue)
							}

							previousValues[key] = existing
						}

						state.Metadata[key] = value
					}

					s.PutTransactionState(txKey, state)
				}
			}
		}
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{
				Target:         sm.GetTarget(),
				Metadata:       sm.GetMetadata(),
				PreviousValues: previousValues,
			},
		},
	}
}

// processMirrorDeletedMetadata applies metadata deletion from a v2 DELETE_METADATA log.
func (p *RequestProcessor) processMirrorDeletedMetadata(ledger string, ledgerID uint32, _ *raftcmdpb.LedgerBoundaries, dm *raftcmdpb.MirrorDeletedMetadata, s InMemoryStore) *commonpb.LedgerLogPayload {
	var previousValue *commonpb.MetadataValue

	if dm.GetTarget() != nil {
		switch target := dm.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{LedgerID: ledgerID, Account: target.Account.GetAddr()},
				Key:        dm.GetKey(),
			}

			if oldVal, err := s.GetAccountMetadata(metaKey); err == nil {
				previousValue = oldVal
			}

			s.DeleteAccountMetadata(metaKey)
		case *commonpb.Target_Transaction:
			txKey := domain.TransactionKey{LedgerID: ledgerID, ID: target.Transaction.GetId()}

			state, _ := s.GetTransactionState(txKey)
			if state != nil && state.GetMetadata() != nil {
				state = state.CloneVT()

				if val, ok := state.GetMetadata()[dm.GetKey()]; ok {
					previousValue = val
					delete(state.GetMetadata(), dm.GetKey())
				}

				s.PutTransactionState(txKey, state)
			}
		}
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{
				Target:        dm.GetTarget(),
				Key:           dm.GetKey(),
				PreviousValue: previousValue,
			},
		},
	}
}

// processMirrorRevertedTransaction processes a v2 REVERTED_TRANSACTION log.
// Missing volumes are auto-initialized to zero so reverse postings are never silently skipped.
func (p *RequestProcessor) processMirrorRevertedTransaction(ledger string, ledgerID uint32, boundaries *raftcmdpb.LedgerBoundaries, rt *raftcmdpb.MirrorRevertedTransaction, s InMemoryStore) (*commonpb.LedgerLogPayload, domain.Describable) {
	// Apply reversed postings with force=true (auto-init missing volumes)
	for _, posting := range rt.GetReversePostings() {
		if err := applyPosting(s, ledgerID, posting, true, p.assetCache); err != nil {
			return nil, err
		}
	}

	// Mark original transaction as reverted
	s.PutReverted(domain.TransactionKey{LedgerID: ledgerID, ID: rt.GetRevertedTransactionId()}, true)

	revertTxID := rt.GetNewTransactionId()
	// Ensure NextTransactionId is past this ID
	if boundaries.GetNextTransactionId() <= revertTxID {
		boundaries.NextTransactionId = revertTxID + 1
	}
	boundaries.PostingCount += uint64(len(rt.GetReversePostings()))
	boundaries.RevertCount++

	// Update the original transaction's state to record the reversion
	origKey := domain.TransactionKey{LedgerID: ledgerID, ID: rt.GetRevertedTransactionId()}

	origState, _ := s.GetTransactionState(origKey)
	if origState != nil {
		origState = origState.CloneVT()
		origState.RevertedByTransaction = revertTxID
		s.PutTransactionState(origKey, origState)
	}

	// Store the revert transaction's state (include metadata from the mirror revert)
	s.PutTransactionState(domain.TransactionKey{LedgerID: ledgerID, ID: revertTxID}, &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Metadata:     rt.GetMetadata(),
	})

	timestamp := rt.GetTimestamp()
	if timestamp == nil {
		timestamp = s.GetDate()
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_RevertedTransaction{
			RevertedTransaction: &commonpb.RevertedTransaction{
				RevertedTransactionId: rt.GetRevertedTransactionId(),
				RevertTransaction: &commonpb.Transaction{
					Postings:   rt.GetReversePostings(),
					Metadata:   rt.GetMetadata(),
					Timestamp:  timestamp,
					Id:         revertTxID,
					InsertedAt: s.GetDate(),
					UpdatedAt:  s.GetDate(),
				},
			},
		},
	}, nil
}

// processPromoteLedger promotes a mirror ledger to normal mode.
func (p *RequestProcessor) processPromoteLedger(order *raftcmdpb.PromoteLedgerOrder, s InMemoryStore) (*commonpb.LogPayload, domain.Describable) {
	info, ok := s.GetLedger(order.GetLedger())
	if !ok {
		return nil, &domain.ErrLedgerNotFound{Name: order.GetLedger()}
	}

	if info.GetMode() != commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.GetLedger()}
	}

	info = info.CloneVT()
	info.Mode = commonpb.LedgerMode_LEDGER_MODE_NORMAL
	info.MirrorSource = nil
	s.PutLedger(order.GetLedger(), info)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_PromoteLedger{
			PromoteLedger: &commonpb.PromotedLedgerLog{
				Name: info.GetName(),
			},
		},
	}, nil
}
