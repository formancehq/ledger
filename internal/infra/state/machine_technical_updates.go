package state

import (
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/domain/processing"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// saveLedgerWithCache updates a LedgerInfo in the in-memory cache, the 0xFF
// cache zone, and the ZoneGlobal durable store — all in the same Pebble batch.
// This replaces the previous pattern of Registry.Ledgers.Put + SaveLedger which
// wrote to memory and ZoneGlobal but skipped 0xFF, causing cache divergence
// after checkpoint restores.
func (fsm *Machine) saveLedgerWithCache(batch *dal.Batch, ledgerKey domain.LedgerKey, info *commonpb.LedgerInfo) error {
	_, idWithTag, err := fsm.Registry.Ledgers.Put(ledgerKey.Bytes(), info)
	if err != nil {
		return fmt.Errorf("updating ledger info in cache: %w", err)
	}

	genByte := byte(fsm.Registry.Cache.CurrentGeneration() % 2)

	valueBytes, err := info.MarshalVT()
	if err != nil {
		return fmt.Errorf("marshaling ledger info for cache: %w", err)
	}

	if err := writeCacheRaw(batch, genByte, dal.SubAttrLedger, idWithTag.ID, idWithTag.Tag, valueBytes); err != nil {
		return fmt.Errorf("persisting ledger info to cache zone: %w", err)
	}

	if err := SaveLedger(batch, info); err != nil {
		return fmt.Errorf("persisting ledger info: %w", err)
	}

	return nil
}

// applyTechnicalUpdates applies Proposal-level technical updates (metadata
// conversions, index ready notifications) that bypass the Order/Log system.
// These are applied directly to Pebble, similar to mirror/sink updates.
func (fsm *Machine) applyTechnicalUpdates(batch *dal.Batch, proposal *raftcmdpb.Proposal) (*ApplyResult, error) {
	for _, convBatch := range proposal.GetMetadataConversionBatches() {
		if err := fsm.applyMetadataConversionBatch(batch, convBatch); err != nil {
			return nil, fmt.Errorf("applying metadata conversion batch: %w", err)
		}
	}

	for _, complete := range proposal.GetMetadataConversionsComplete() {
		fsm.applyMetadataConversionCompletion(batch, complete)
	}

	for _, ready := range proposal.GetIndexReadyUpdates() {
		fsm.applyIndexReady(batch, ready)
	}

	return &ApplyResult{ProposalID: proposal.GetId()}, nil
}

// applyMetadataConversionBatch applies a background metadata conversion batch.
// No log entry is produced.
func (fsm *Machine) applyMetadataConversionBatch(batch *dal.Batch, b *raftcmdpb.MetadataConversionBatch) error {
	ledgerKey := domain.LedgerKey{Name: b.GetLedger()}

	info, _, err := fsm.Registry.Ledgers.Get(ledgerKey.Bytes())
	if err != nil || info == nil {
		return nil // ledger not found or deleted; ignore
	}

	info = info.CloneVT()

	// Staleness check: the field must still be CONVERTING with the expected type.
	_, fieldSchema := processing.SchemaFieldForTarget(info.GetMetadataSchema(), b.GetTargetType(), b.GetKey())
	if fieldSchema == nil ||
		fieldSchema.GetStatus() != commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
		fieldSchema.GetType() != b.GetExpectedType() {
		return nil // stale batch
	}

	for _, entry := range b.GetEntries() {
		value, err := fsm.getConvertBatchValue(b.GetTargetType(), entry.GetCanonicalKey())
		if err != nil {
			return err
		}

		if value == nil {
			continue // key deleted since scan
		}

		if !commonpb.TypeMatches(value, b.GetExpectedType()) {
			if err := fsm.putConvertBatchValue(batch, b.GetTargetType(), entry.GetCanonicalKey(), entry.GetConvertedValue()); err != nil {
				return err
			}
		}
	}

	// Persist conversion progress in the schema.
	fieldSchema.TotalKeys = b.GetTotalKeys()
	fieldSchema.ConvertedKeys = b.GetConvertedKeysSoFar()

	return fsm.saveLedgerWithCache(batch, ledgerKey, info)
}

// applyMetadataConversionCompletion applies a metadata conversion completion.
// No log entry is produced.
func (fsm *Machine) applyMetadataConversionCompletion(batch *dal.Batch, complete *raftcmdpb.MetadataConversionCompletion) {
	ledgerKey := domain.LedgerKey{Name: complete.GetLedger()}

	info, _, err := fsm.Registry.Ledgers.Get(ledgerKey.Bytes())
	if err != nil || info == nil {
		return // ledger not found; ignore
	}

	info = info.CloneVT()

	_, fieldSchema := processing.SchemaFieldForTarget(info.GetMetadataSchema(), complete.GetTargetType(), complete.GetKey())
	if fieldSchema == nil ||
		fieldSchema.GetStatus() != commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING ||
		fieldSchema.GetType() != complete.GetExpectedType() {
		return // stale
	}

	fieldSchema.Status = commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE
	fieldSchema.ConvertedKeys = fieldSchema.GetTotalKeys()

	_ = fsm.saveLedgerWithCache(batch, ledgerKey, info)
}

// applyIndexReady applies an index-ready notification. No log entry is produced.
// The index builder detects the status change by reading LedgerInfo on its next tick.
func (fsm *Machine) applyIndexReady(batch *dal.Batch, ready *raftcmdpb.IndexReadyUpdate) {
	ledgerKey := domain.LedgerKey{Name: ready.GetLedger()}

	info, _, err := fsm.Registry.Ledgers.Get(ledgerKey.Bytes())
	if err != nil || info == nil {
		return // ledger not found; ignore
	}

	info = info.CloneVT()

	switch idx := ready.GetIndex().(type) {
	case *raftcmdpb.IndexReadyUpdate_Transaction:
		switch kind := idx.Transaction.GetKind().(type) {
		case *commonpb.TransactionIndex_Builtin:
			if info.GetBuiltinIndexes() != nil {
				processing.SetBuiltinStatus(info.GetBuiltinIndexes(), kind.Builtin, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY)
			}
		case *commonpb.TransactionIndex_MetadataKey:
			processing.ProcessIndexReadyMetadata(info, commonpb.TargetType_TARGET_TYPE_TRANSACTION, kind.MetadataKey)
		}
	case *raftcmdpb.IndexReadyUpdate_Account:
		switch kind := idx.Account.GetKind().(type) {
		case *commonpb.AccountIndex_Builtin:
			_ = kind // No account builtins yet
		case *commonpb.AccountIndex_MetadataKey:
			processing.ProcessIndexReadyMetadata(info, commonpb.TargetType_TARGET_TYPE_ACCOUNT, kind.MetadataKey)
		}
	case *raftcmdpb.IndexReadyUpdate_LogBuiltin:
		if info.GetLogBuiltinIndexes() != nil {
			processing.SetLogBuiltinStatus(info.GetLogBuiltinIndexes(), idx.LogBuiltin, commonpb.IndexBuildStatus_INDEX_BUILD_STATUS_READY)
		}
	}

	_ = fsm.saveLedgerWithCache(batch, ledgerKey, info)
}

// getConvertBatchValue retrieves the current metadata value for a canonical key,
// dispatching to the correct Registry store based on target type.
func (fsm *Machine) getConvertBatchValue(targetType commonpb.TargetType, canonicalKey []byte) (*commonpb.MetadataValue, error) {
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		v, _, err := fsm.Registry.AccountMetadata.Get(canonicalKey)
		if err != nil {
			return nil, nil //nolint:nilerr // key not found = deleted since scan
		}

		return v, nil
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		v, _, err := fsm.Registry.LedgerMetadata.Get(canonicalKey)
		if err != nil {
			return nil, nil //nolint:nilerr // key not found = deleted since scan
		}

		return v, nil
	default:
		return nil, nil
	}
}

// putConvertBatchValue stores a converted metadata value in the Registry,
// dispatching to the correct store based on target type.
func (fsm *Machine) putConvertBatchValue(batch *dal.Batch, targetType commonpb.TargetType, canonicalKey []byte, value *commonpb.MetadataValue) error {
	switch targetType {
	case commonpb.TargetType_TARGET_TYPE_ACCOUNT:
		if _, _, err := fsm.Registry.AccountMetadata.Put(canonicalKey, value); err != nil {
			return fmt.Errorf("setting account metadata in cache: %w", err)
		}

		if _, err := fsm.Registry.Attrs.Metadata.Set(batch, canonicalKey, value); err != nil {
			return fmt.Errorf("persisting account metadata: %w", err)
		}
	case commonpb.TargetType_TARGET_TYPE_LEDGER:
		if _, _, err := fsm.Registry.LedgerMetadata.Put(canonicalKey, value); err != nil {
			return fmt.Errorf("setting ledger metadata in cache: %w", err)
		}

		if _, err := fsm.Registry.Attrs.LedgerMetadata.Set(batch, canonicalKey, value); err != nil {
			return fmt.Errorf("persisting ledger metadata: %w", err)
		}
	}

	return nil
}
