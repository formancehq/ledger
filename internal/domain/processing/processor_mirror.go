package processing

import (
	"errors"
	"maps"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// processMirrorIngest processes a single MirrorIngestOrder.
// It handles one v2 log entry: fill gaps, create transactions, save/delete metadata, reverts.
// The ledger must be in MIRROR mode. As an orchestrator it populates
// ctx.Boundaries (and ctx.LedgerInfo) before dispatching to apply-child
// handlers so children consume everything through a single uniform Context.
// Mirror replays do NOT re-run account-type validation: they are
// exactly what the source ledger committed (parity > re-checking).
func processMirrorIngest(ledger string, order *raftcmdpb.MirrorIngestOrder, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	if info.GetMode() != commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: ledger}
	}
	// Re-touch ledger info so it enters the Merge buffer and gets propagated
	// back to Gen0 on commit. Without this, ledger info is evicted after two
	// cache rotations because mirror proposals bypass the admission preloader.
	s.Ledgers().Put(domain.LedgerKey{Name: ledger}, info)

	boundariesReader, loadErr := loadBoundaries(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	boundaries := boundariesReader.Mutate()

	// Stage per-apply context fields for child handlers.
	ctx.Boundaries = boundaries
	ctx.LedgerInfo = info

	entry := order.GetEntry()
	if entry == nil {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: ledger}
	}

	var logPayload *commonpb.LedgerLogPayload

	switch data := entry.GetData().(type) {
	case *raftcmdpb.MirrorLogEntry_FillGap:
		logPayload = processMirrorFillGap(data.FillGap, entry.GetV2LogId(), ctx)

	case *raftcmdpb.MirrorLogEntry_CreatedTransaction:
		var err domain.Describable

		logPayload, err = processMirrorCreatedTransaction(ledger, data.CreatedTransaction, ctx)
		if err != nil {
			return nil, err
		}

	case *raftcmdpb.MirrorLogEntry_SavedMetadata:
		var err domain.Describable

		logPayload, err = processMirrorSavedMetadata(ledger, data.SavedMetadata, ctx)
		if err != nil {
			return nil, err
		}

	case *raftcmdpb.MirrorLogEntry_DeletedMetadata:
		var err domain.Describable

		logPayload, err = processMirrorDeletedMetadata(ledger, data.DeletedMetadata, ctx)
		if err != nil {
			return nil, err
		}

	case *raftcmdpb.MirrorLogEntry_RevertedTransaction:
		var err domain.Describable

		logPayload, err = processMirrorRevertedTransaction(ledger, data.RevertedTransaction, ctx)
		if err != nil {
			return nil, err
		}

	default:
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: ledger}
	}

	// Assign per-ledger log ID and advance boundaries
	nextLogID := boundaries.GetNextLogId()
	boundaries.NextLogId = nextLogID + 1
	s.Boundaries().Put(domain.LedgerKey{Name: ledger}, boundaries)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log: &commonpb.LedgerLog{
					Data: logPayload,
					Date: s.GetDate().Mutate(),
					Id:   nextLogID,
				},
			},
		},
	}, nil
}

// processMirrorFillGap creates a FilledGapLog for a v2 log that has no v3 equivalent.
// It also advances NextTransactionId for any skipped transaction IDs.
//
// Signature deviates from the uniform `(order, ctx)` shape because the
// v2LogID belongs to the wrapping MirrorLogEntry, not the FillGap message
// itself — passing it as an extra arg avoids reaching back into the entry.
func processMirrorFillGap(gap *raftcmdpb.MirrorFillGap, v2LogID uint64, ctx *Context) *commonpb.LedgerLogPayload {
	// Advance NextTransactionId for each skipped transaction
	for range gap.GetSkippedTransactionIds() {
		ctx.Boundaries.NextTransactionId++
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
func processMirrorCreatedTransaction(ledger string, ct *raftcmdpb.MirrorCreatedTransaction, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	boundaries := ctx.Boundaries
	s := ctx.Scope

	// Apply each posting with force=true (skip balance checks, auto-init missing volumes)
	for _, posting := range ct.GetPostings() {
		if err := applyPosting(s, ledger, posting, true, ctx.AssetCache); err != nil {
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
		timestamp = s.GetDate().Mutate()
	}

	// Record transaction state (include metadata from the mirrored transaction)
	s.TransactionStates().Put(domain.TransactionKey{LedgerName: ledger, ID: txID}, &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Metadata:     ct.GetMetadata(),
		Timestamp:    timestamp,
		Postings:     ct.GetPostings(),
	})

	// Store reference if provided
	if ct.GetReference() != "" {
		s.TransactionReferences().Put(
			domain.TransactionReferenceKey{LedgerName: ledger, Reference: ct.GetReference()},
			&commonpb.TransactionReferenceValue{TransactionId: txID},
		)
	}

	// Store account metadata. Previous values are no longer captured: the
	// indexer resolves prior encoded values via the reverse map on apply.
	var accountMetadata map[string]*commonpb.MetadataMap

	if len(ct.GetAccountMetadata()) > 0 {
		accountMetadata = ct.GetAccountMetadata()
		for account, mm := range ct.GetAccountMetadata() {
			for key, value := range mm.GetValues() {
				metaKey := domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: ledger, Account: account},
					Key:        key,
				}

				s.AccountMetadata().Put(metaKey, value)
			}
		}
	}

	var chapterID uint64
	if p, ok := s.GetCurrentOpenChapter(); ok {
		chapterID = p.GetId()
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
					InsertedAt: s.GetDate().Mutate(),
					UpdatedAt:  s.GetDate().Mutate(),
				},
				AccountMetadata: accountMetadata,
				ChapterId:       chapterID,
			},
		},
	}, nil
}

// processMirrorSavedMetadata applies metadata from a v2 SET_METADATA log.
//
// Previous values are no longer captured into the log: the indexer
// resolves prior encoded values via the reverse map on apply.
func processMirrorSavedMetadata(ledger string, sm *raftcmdpb.MirrorSavedMetadata, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	s := ctx.Scope

	if sm.GetTarget() != nil {
		switch target := sm.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			for key, value := range sm.GetMetadata() {
				metaKey := domain.MetadataKey{
					AccountKey: domain.AccountKey{LedgerName: ledger, Account: target.Account.GetAddr()},
					Key:        key,
				}
				s.AccountMetadata().Put(metaKey, value)
			}
		case *commonpb.Target_TransactionId:
			if len(sm.GetMetadata()) > 0 {
				txKey := domain.TransactionKey{LedgerName: ledger, ID: target.TransactionId}

				stateReader, err := s.TransactionStates().Get(txKey)
				if err != nil && !errors.Is(err, domain.ErrNotFound) {
					return nil, &domain.ErrStorageOperation{Operation: "reading transaction state", Cause: err}
				}

				if stateReader != nil {
					state := stateReader.Mutate()

					if state.GetMetadata() == nil {
						state.Metadata = make(map[string]*commonpb.MetadataValue)
					}

					maps.Copy(state.GetMetadata(), sm.GetMetadata())

					s.TransactionStates().Put(txKey, state)
				}
			}
		}
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{
			SavedMetadata: &commonpb.SavedMetadata{
				Target:   sm.GetTarget(),
				Metadata: sm.GetMetadata(),
			},
		},
	}, nil
}

// processMirrorDeletedMetadata applies metadata deletion from a v2 DELETE_METADATA log.
//
// Previous values are no longer captured into the log: the indexer
// resolves prior encoded values via the reverse map on apply.
func processMirrorDeletedMetadata(ledger string, dm *raftcmdpb.MirrorDeletedMetadata, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	s := ctx.Scope

	if dm.GetTarget() != nil {
		switch target := dm.GetTarget().GetTarget().(type) {
		case *commonpb.Target_Account:
			metaKey := domain.MetadataKey{
				AccountKey: domain.AccountKey{LedgerName: ledger, Account: target.Account.GetAddr()},
				Key:        dm.GetKey(),
			}
			if err := s.AccountMetadata().Delete(metaKey); err != nil {
				return nil, &domain.ErrStorageOperation{Operation: "deleting account metadata", Cause: err}
			}
		case *commonpb.Target_TransactionId:
			txKey := domain.TransactionKey{LedgerName: ledger, ID: target.TransactionId}

			stateReader, err := s.TransactionStates().Get(txKey)
			if err != nil && !errors.Is(err, domain.ErrNotFound) {
				return nil, &domain.ErrStorageOperation{Operation: "reading transaction state", Cause: err}
			}

			if stateReader != nil && stateReader.GetMetadata() != nil {
				state := stateReader.Mutate()

				delete(state.GetMetadata(), dm.GetKey())

				s.TransactionStates().Put(txKey, state)
			}
		}
	}

	return &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_DeletedMetadata{
			DeletedMetadata: &commonpb.DeletedMetadata{
				Target: dm.GetTarget(),
				Key:    dm.GetKey(),
			},
		},
	}, nil
}

// processMirrorRevertedTransaction processes a v2 REVERTED_TRANSACTION log.
// Missing volumes are auto-initialized to zero so reverse postings are never silently skipped.
func processMirrorRevertedTransaction(ledger string, rt *raftcmdpb.MirrorRevertedTransaction, ctx *Context) (*commonpb.LedgerLogPayload, domain.Describable) {
	boundaries := ctx.Boundaries
	s := ctx.Scope

	// Apply reversed postings with force=true (auto-init missing volumes)
	for _, posting := range rt.GetReversePostings() {
		if err := applyPosting(s, ledger, posting, true, ctx.AssetCache); err != nil {
			return nil, err
		}
	}

	// Mark original transaction as reverted
	s.PutReverted(domain.TransactionKey{LedgerName: ledger, ID: rt.GetRevertedTransactionId()}, true)

	revertTxID := rt.GetNewTransactionId()
	// Ensure NextTransactionId is past this ID
	if boundaries.GetNextTransactionId() <= revertTxID {
		boundaries.NextTransactionId = revertTxID + 1
	}
	boundaries.PostingCount += uint64(len(rt.GetReversePostings()))
	boundaries.RevertCount++

	// Update the original transaction's state to record the reversion
	origKey := domain.TransactionKey{LedgerName: ledger, ID: rt.GetRevertedTransactionId()}

	origReader, err := s.TransactionStates().Get(origKey)
	if err != nil && !errors.Is(err, domain.ErrNotFound) {
		return nil, &domain.ErrStorageOperation{Operation: "reading original transaction state", Cause: err}
	}

	if origReader != nil {
		origState := origReader.Mutate()
		origState.RevertedByTransaction = revertTxID
		s.TransactionStates().Put(origKey, origState)
	}

	timestamp := rt.GetTimestamp()
	if timestamp == nil {
		timestamp = s.GetDate().Mutate()
	}

	// Store the revert transaction's state (include metadata from the mirror revert)
	s.TransactionStates().Put(domain.TransactionKey{LedgerName: ledger, ID: revertTxID}, &commonpb.TransactionState{
		CreatedByLog: s.GetNextSequenceID(),
		Metadata:     rt.GetMetadata(),
		Timestamp:    timestamp,
		Postings:     rt.GetReversePostings(),
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
					InsertedAt: s.GetDate().Mutate(),
					UpdatedAt:  s.GetDate().Mutate(),
				},
			},
		},
	}, nil
}

// processPromoteLedger promotes a mirror ledger to normal mode. The
// MirrorConfigChange signal (post-commit mirror worker reconciliation)
// is derived from the PromotedLedgerLog by deriveSignals.
func processPromoteLedger(ledger string, ctx *Context) (*commonpb.LogPayload, domain.Describable) {
	s := ctx.Scope

	info, loadErr := loadLedger(s, ledger)
	if loadErr != nil {
		return nil, loadErr
	}

	if info.GetMode() != commonpb.LedgerMode_LEDGER_MODE_MIRROR {
		return nil, &domain.ErrLedgerNotInMirrorMode{Name: ledger}
	}

	info.Mode = commonpb.LedgerMode_LEDGER_MODE_NORMAL
	info.MirrorSource = nil
	s.Ledgers().Put(domain.LedgerKey{Name: ledger}, info)

	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_PromoteLedger{
			PromoteLedger: &commonpb.PromotedLedgerLog{
				Name: info.GetName(),
			},
		},
	}, nil
}
