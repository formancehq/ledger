package processing

import (
	"errors"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processMirrorIngest processes a single MirrorIngestOrder.
// It handles one v2 log entry: fill gaps, create transactions, save/delete metadata, reverts.
// The ledger must be in MIRROR mode.
func (p *RequestProcessor) processMirrorIngest(order *raftcmdpb.MirrorIngestOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	info, loadErr := loadLedger(s, order.GetLedger())
	if loadErr != nil {
		return nil, loadErr
	}

	if info.GetMode() != commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.GetLedger()}
	}
	// Re-touch ledger info so it enters the Merge buffer and gets propagated
	// back to Gen0 on commit. Without this, ledger info is evicted after two
	// cache rotations because mirror proposals bypass the admission preloader.
	s.PutLedger(order.GetLedger(), info)

	boundariesReader, loadErr := loadBoundaries(s, order.GetLedger())
	if loadErr != nil {
		return nil, loadErr
	}

	boundaries := boundariesReader.Mutate()

	entry := order.GetEntry()
	if entry == nil {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: order.GetLedger()}
	}

	ledgerName := order.GetLedger()

	var logPayload *commonpb.LedgerLogPayload

	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_FillGap:
		logPayload = p.processMirrorFillGap(ledgerName, boundaries, data.FillGap, entry.GetV2LogId(), s)

	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		var err domain.Describable

		logPayload, err = p.processMirrorCreatedTransaction(ledgerName, boundaries, data.CreatedTransaction, s)
		if err != nil {
			return nil, err
		}

	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		var err domain.Describable

		logPayload, err = p.processMirrorSavedMetadata(ledgerName, data.SavedMetadata, s)
		if err != nil {
			return nil, err
		}

	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		var err domain.Describable

		logPayload, err = p.processMirrorDeletedMetadata(ledgerName, data.DeletedMetadata, s)
		if err != nil {
			return nil, err
		}

	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		var err domain.Describable

		logPayload, err = p.processMirrorRevertedTransaction(ledgerName, boundaries, data.RevertedTransaction, s)
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
func (p *RequestProcessor) processMirrorFillGap(ledger string, boundaries *raftcmdpb.LedgerBoundaries, gap *raftcmdpb.MirrorFillGap, v2LogID uint64, s Scope) *commonpb.LedgerLogPayload {
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
func (p *RequestProcessor) processMirrorCreatedTransaction(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, ct *raftcmdpb.MirrorCreatedTransaction, s Scope) (*commonpb.LedgerLogPayload, domain.Describable) {
	// Apply each posting with force=true (skip balance checks, auto-init missing volumes)
	for _, posting := range ct.GetPostings() {
		if err := applyPosting(s, ledgerName, posting, true, p.assetCache); err != nil {
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

	timestamp := ct.GetTimestamp()
	if timestamp == nil {
		timestamp = s.GetDate()
	}

	// Record transaction state (include metadata from the mirrored transaction)
	s.PutTransactionState(domain.TransactionKey{LedgerName: ledgerName, ID: txID}, &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Metadata:     ct.GetMetadata(),
		Timestamp:    timestamp,
	})

	// Store reference if provided
	if ct.GetReference() != "" {
		s.PutTransactionReference(
			domain.TransactionReferenceKey{LedgerName: ledgerName, Reference: ct.GetReference()},
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
					AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: account},
					Key:        key,
				}

				// Capture old value before overwriting (for log replay in indexbuilder).
				oldVal, err := s.GetAccountMetadata(metaKey)
				if err != nil && !errors.Is(err, domain.ErrNotFound) {
					return nil, &domain.ErrStorageOperation{Operation: "reading previous account metadata", Cause: err}
				}

				if err == nil {
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
func (p *RequestProcessor) processMirrorSavedMetadata(ledgerName string, sm *raftcmdpb.MirrorSavedMetadata, s Scope) (*commonpb.LedgerLogPayload, domain.Describable) {
	var previousValues map[string]*commonpb.MetadataValue

	if sm.GetTarget() != nil {
		switch target := sm.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			for key, value := range sm.GetMetadata() {
				metaKey := domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: target.Account.GetAddr()},
					Key:        key,
				}

				// Capture old value before overwriting.
				oldVal, err := s.GetAccountMetadata(metaKey)
				if err != nil && !errors.Is(err, domain.ErrNotFound) {
					return nil, &domain.ErrStorageOperation{Operation: "reading previous account metadata", Cause: err}
				}

				if err == nil {
					if previousValues == nil {
						previousValues = make(map[string]*commonpb.MetadataValue)
					}

					previousValues[key] = oldVal
				}

				s.PutAccountMetadata(metaKey, value)
			}
		case *commonpb.Target_TransactionId:
			if len(sm.GetMetadata()) > 0 {
				txKey := domain.TransactionKey{LedgerName: ledgerName, ID: target.TransactionId}

				state, err := s.GetTransactionState(txKey)
				if err != nil && !errors.Is(err, domain.ErrNotFound) {
					return nil, &domain.ErrStorageOperation{Operation: "reading transaction state", Cause: err}
				}

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
	}, nil
}

// processMirrorDeletedMetadata applies metadata deletion from a v2 DELETE_METADATA log.
func (p *RequestProcessor) processMirrorDeletedMetadata(ledgerName string, dm *raftcmdpb.MirrorDeletedMetadata, s Scope) (*commonpb.LedgerLogPayload, domain.Describable) {
	var previousValue *commonpb.MetadataValue

	if dm.GetTarget() != nil {
		switch target := dm.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{LedgerName: ledgerName, Account: target.Account.GetAddr()},
				Key:        dm.GetKey(),
			}

			oldVal, err := s.GetAccountMetadata(metaKey)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrStorageOperation{Operation: "reading previous account metadata", Cause: err}
			}

			if err == nil {
				previousValue = oldVal
			}

			s.DeleteAccountMetadata(metaKey)
		case *commonpb.Target_TransactionId:
			txKey := domain.TransactionKey{LedgerName: ledgerName, ID: target.TransactionId}

			state, err := s.GetTransactionState(txKey)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrStorageOperation{Operation: "reading transaction state", Cause: err}
			}

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
	}, nil
}

// processMirrorRevertedTransaction processes a v2 REVERTED_TRANSACTION log.
// Missing volumes are auto-initialized to zero so reverse postings are never silently skipped.
func (p *RequestProcessor) processMirrorRevertedTransaction(ledgerName string, boundaries *raftcmdpb.LedgerBoundaries, rt *raftcmdpb.MirrorRevertedTransaction, s Scope) (*commonpb.LedgerLogPayload, domain.Describable) {
	// Apply reversed postings with force=true (auto-init missing volumes)
	for _, posting := range rt.GetReversePostings() {
		if err := applyPosting(s, ledgerName, posting, true, p.assetCache); err != nil {
			return nil, err
		}
	}

	// Mark original transaction as reverted
	s.PutReverted(domain.TransactionKey{LedgerName: ledgerName, ID: rt.GetRevertedTransactionId()}, true)

	revertTxID := rt.GetNewTransactionId()
	// Ensure NextTransactionId is past this ID
	if boundaries.GetNextTransactionId() <= revertTxID {
		boundaries.NextTransactionId = revertTxID + 1
	}
	boundaries.PostingCount += uint64(len(rt.GetReversePostings()))
	boundaries.RevertCount++

	// Update the original transaction's state to record the reversion
	origKey := domain.TransactionKey{LedgerName: ledgerName, ID: rt.GetRevertedTransactionId()}

	origState, err := s.GetTransactionState(origKey)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrStorageOperation{Operation: "reading original transaction state", Cause: err}
	}

	if origState != nil {
		origState = origState.CloneVT()
		origState.RevertedByTransaction = revertTxID
		s.PutTransactionState(origKey, origState)
	}

	timestamp := rt.GetTimestamp()
	if timestamp == nil {
		timestamp = s.GetDate()
	}

	// Store the revert transaction's state (include metadata from the mirror revert)
	s.PutTransactionState(domain.TransactionKey{LedgerName: ledgerName, ID: revertTxID}, &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Metadata:     rt.GetMetadata(),
		Timestamp:    timestamp,
	})

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
func (p *RequestProcessor) processPromoteLedger(order *raftcmdpb.PromoteLedgerOrder, s Scope) (*commonpb.LogPayload, domain.Describable) {
	info, loadErr := loadLedger(s, order.GetLedger())
	if loadErr != nil {
		return nil, loadErr
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
