package state

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// AppendLogs appends system logs to the batch.
func AppendLogs(b *dal.WriteSession, logs []*commonpb.Log) error {
	for _, log := range logs {
		b.KeyBuilder.
			PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
			PutUint64(log.GetSequence())

		key := b.KeyBuilder.Consume()

		if err := b.SetProto(key, log); err != nil {
			return fmt.Errorf("inserting system log: %w", err)
		}
	}

	return nil
}

// SaveLedger saves or updates a ledger in the store.
func SaveLedger(b *dal.WriteSession, info *commonpb.LedgerInfo) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZoneGlobal, dal.SubGlobLedgerInfo).
		PutLedgerName(info.GetName())

	err := b.SetProto(b.KeyBuilder.Consume(), info)
	if err != nil {
		return fmt.Errorf("inserting ledger info: %w", err)
	}

	return nil
}

// SaveNextLedgerID persists the next ledger ID counter to Pebble.
func saveNextLedgerID(b *dal.WriteSession, nextID uint32) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobNextLedgerID)

	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], nextID)

	return b.SetBytes(b.KeyBuilder.Consume(), buf[:])
}

// AppendAuditEntries appends audit entries to the batch.
func appendAuditEntries(b *dal.WriteSession, entries ...*auditpb.AuditEntry) error {
	for _, entry := range entries {
		b.KeyBuilder.
			PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
			PutUint64(entry.GetSequence())

		err := b.SetProto(b.KeyBuilder.Consume(), entry)
		if err != nil {
			return fmt.Errorf("inserting audit entry: %w", err)
		}
	}

	return nil
}

// AppendAuditItems appends per-order audit items to the batch.
// Key format: [ZoneCold][SubColdAuditItem][audit_sequence BE 8][order_index BE 4].
func appendAuditItems(b *dal.WriteSession, auditSequence uint64, items ...*auditpb.AuditItem) error {
	for _, item := range items {
		b.KeyBuilder.
			PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).
			PutUint64(auditSequence).
			PutUint32(item.GetOrderIndex())

		err := b.SetProto(b.KeyBuilder.Consume(), item)
		if err != nil {
			return fmt.Errorf("inserting audit item: %w", err)
		}
	}

	return nil
}

// SaveSigningKey stores an Ed25519 public key in the batch.
// The value format is [publicKey (32 bytes)][parentKeyID (UTF-8 variable)].
// Backward-compatible: existing 32-byte values are treated as root keys (no parent).
func SaveSigningKey(b *dal.WriteSession, keyID string, publicKey []byte, parentKeyID string) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSigningKey).
		PutString(keyID)

	value := publicKey
	if parentKeyID != "" {
		value = make([]byte, len(publicKey)+len(parentKeyID))
		copy(value, publicKey)
		copy(value[len(publicKey):], parentKeyID)
	}

	err := b.SetBytes(b.KeyBuilder.Consume(), value)
	if err != nil {
		return fmt.Errorf("saving signing key: %w", err)
	}

	return nil
}

// DeleteSigningKey removes a signing key from the batch.
func DeleteSigningKey(b *dal.WriteSession, keyID string) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSigningKey).
		PutString(keyID)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("deleting signing key: %w", err)
	}

	return nil
}

// SaveSigningConfig stores the require-signatures flag in the batch.
func SaveSigningConfig(b *dal.WriteSession, requireSignatures bool) error {
	value := []byte{0x00}
	if requireSignatures {
		value[0] = 0x01
	}

	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobSigningConfig}, value)
	if err != nil {
		return fmt.Errorf("saving signing config: %w", err)
	}

	return nil
}

// SaveMaintenanceMode stores the maintenance mode flag in the batch.
func SaveMaintenanceMode(b *dal.WriteSession, enabled bool) error {
	value := []byte{0x00}
	if enabled {
		value[0] = 0x01
	}

	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobMaintenanceMode}, value)
	if err != nil {
		return fmt.Errorf("saving maintenance mode: %w", err)
	}

	return nil
}

// SaveClusterState stores the persisted cluster state in the batch.
func saveClusterState(b *dal.WriteSession, state *commonpb.PersistedClusterState) error {
	return b.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterConfig}, state)
}

// SavePeriodSchedule stores the period schedule cron expression in the batch.
func SavePeriodSchedule(b *dal.WriteSession, cron string) error {
	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobPeriodSchedule}, []byte(cron))
	if err != nil {
		return fmt.Errorf("saving period schedule: %w", err)
	}

	return nil
}

// BatchDeletePeriodSchedule removes the period schedule from the batch.
func batchDeletePeriodSchedule(b *dal.WriteSession) error {
	err := b.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobPeriodSchedule})
	if err != nil {
		return fmt.Errorf("deleting period schedule: %w", err)
	}

	return nil
}

// SaveQueryCheckpointSchedule stores the query checkpoint schedule cron expression in the batch.
func SaveQueryCheckpointSchedule(b *dal.WriteSession, cron string) error {
	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobQueryCheckpointSchedule}, []byte(cron))
	if err != nil {
		return fmt.Errorf("saving query checkpoint schedule: %w", err)
	}

	return nil
}

// BatchDeleteQueryCheckpointSchedule removes the query checkpoint schedule from the batch.
func batchDeleteQueryCheckpointSchedule(b *dal.WriteSession) error {
	err := b.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobQueryCheckpointSchedule})
	if err != nil {
		return fmt.Errorf("deleting query checkpoint schedule: %w", err)
	}

	return nil
}

// SaveReversionWord writes a single bitset word for a ledger.
// Key: [0x03][0x01][ledger\x00][wordIndex BE 8 bytes] → [uint64 LE 8 bytes].
func saveReversionWord(b *dal.WriteSession, ledgerID uint32, wordIndex uint64, value uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLReversions).
		PutLedgerID(ledgerID).
		PutUint64(wordIndex)

	if err := b.SetBytes(b.KeyBuilder.Consume(), bitset.MarshalWord(value)); err != nil {
		return fmt.Errorf("saving reversion word %d for ledger %d: %w", wordIndex, ledgerID, err)
	}

	return nil
}

// DeleteReversionsByLedger removes all reversion words for a ledger.
func deleteReversionsByLedger(b *dal.WriteSession, ledgerID uint32) error {
	prefix := make([]byte, 2+4)
	prefix[0] = dal.ZonePerLedger
	prefix[1] = dal.SubPLReversions
	binary.BigEndian.PutUint32(prefix[2:], ledgerID)

	end := make([]byte, 2+4)
	end[0] = dal.ZonePerLedger
	end[1] = dal.SubPLReversions
	binary.BigEndian.PutUint32(end[2:], ledgerID+1)

	return b.DeleteRangeNoSync(prefix, end)
}

// SavePreparedQuery stores a prepared query in the batch.
func SavePreparedQuery(b *dal.WriteSession, pq *commonpb.PreparedQuery) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLPreparedQuery).
		PutLedgerName(pq.GetLedger()).
		PutString(pq.GetName())

	err := b.SetProto(b.KeyBuilder.Consume(), pq)
	if err != nil {
		return fmt.Errorf("saving prepared query: %w", err)
	}

	return nil
}

// SaveQueryCheckpoint stores a query checkpoint state in the batch (Raft-replicated).
func saveQueryCheckpoint(b *dal.WriteSession, cp *raftcmdpb.QueryCheckpointState) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobQueryCheckpoint).
		PutUint64(cp.GetCheckpointId())

	err := b.SetProto(b.KeyBuilder.Consume(), cp)
	if err != nil {
		return fmt.Errorf("saving query checkpoint: %w", err)
	}

	return nil
}

// DeleteQueryCheckpointFromBatch removes a query checkpoint from the batch (Raft-replicated).
func deleteQueryCheckpointFromBatch(b *dal.WriteSession, checkpointID uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobQueryCheckpoint).
		PutUint64(checkpointID)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("deleting query checkpoint: %w", err)
	}

	return nil
}

// StoreNextQueryCheckpointID writes the next query checkpoint ID as 8-byte big-endian uint64.
func storeNextQueryCheckpointID(b *dal.WriteSession, id uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, id)

	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobNextQueryCheckpointID}, value)
	if err != nil {
		return fmt.Errorf("storing next query checkpoint ID: %w", err)
	}

	return nil
}

// SetSinkCursor writes a per-sink events cursor to the batch (Raft-replicated).
func SetSinkCursor(b *dal.WriteSession, sinkName string, sequence uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkCursor).
		PutString(sinkName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sequence)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("setting sink cursor: %w", err)
	}

	return nil
}

// SetSinkStatus writes a per-sink status to the batch (Raft-replicated).
func SetSinkStatus(b *dal.WriteSession, status *commonpb.SinkStatus) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkStatus).
		PutString(status.GetSinkName())

	err := b.SetProto(b.KeyBuilder.Consume(), status)
	if err != nil {
		return fmt.Errorf("setting sink status: %w", err)
	}

	return nil
}

// ClearSinkStatus removes a per-sink status from the batch.
func ClearSinkStatus(b *dal.WriteSession, sinkName string) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkStatus).
		PutString(sinkName)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("clearing sink status: %w", err)
	}

	return nil
}

// StorePeriod marshals and writes a single period keyed by its ID.
func StorePeriod(b *dal.WriteSession, period *commonpb.Period) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZoneGlobal, dal.SubGlobPeriods).
		PutUint64(period.GetId())

	err := b.SetProto(b.KeyBuilder.Consume(), period)
	if err != nil {
		return fmt.Errorf("storing period: %w", err)
	}

	return nil
}

// StoreNextPeriodID writes the next period ID as 8-byte big-endian uint64.
func StoreNextPeriodID(b *dal.WriteSession, id uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, id)

	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobNextPeriodID}, value)
	if err != nil {
		return fmt.Errorf("storing next period ID: %w", err)
	}

	return nil
}

// SetMirrorSourceHead writes the latest known v2 source log count to the batch (Raft-replicated).
func SetMirrorSourceHead(b *dal.WriteSession, ledgerID uint32, count uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorSourceHead).
		PutLedgerID(ledgerID)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], count)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("setting mirror source head: %w", err)
	}

	return nil
}

// SetMirrorCursor writes a per-ledger mirror cursor to the batch (Raft-replicated).
func SetMirrorCursor(b *dal.WriteSession, ledgerID uint32, cursor uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorCursor).
		PutLedgerID(ledgerID)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], cursor)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("setting mirror cursor: %w", err)
	}

	return nil
}

// SetMirrorStatus writes a per-ledger mirror sync error to the batch.
func SetMirrorStatus(b *dal.WriteSession, ledgerID uint32, syncErr *commonpb.MirrorSyncError) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorStatus).
		PutLedgerID(ledgerID)

	err := b.SetProto(b.KeyBuilder.Consume(), syncErr)
	if err != nil {
		return fmt.Errorf("setting mirror status: %w", err)
	}

	return nil
}

// ClearMirrorStatus removes a per-ledger mirror sync error from the batch.
func clearMirrorStatus(b *dal.WriteSession, ledgerID uint32) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorStatus).
		PutLedgerID(ledgerID)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("clearing mirror status: %w", err)
	}

	return nil
}

// SetAppliedIndex writes the last applied Raft index to the batch.
func SetAppliedIndex(b *dal.WriteSession, index uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, index)

	return b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex}, value)
}

// SetLastAppliedTimestamp writes the last applied HLC timestamp to the batch.
func setLastAppliedTimestamp(b *dal.WriteSession, timestamp uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, timestamp)

	return b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobLastAppliedTimestamp}, value)
}

// ledgerScopedAttrTypes lists attribute types that use ledger-scoped canonical keys
// (format [ledger\x00]...). Used by DeleteLedgerData for per-type range deletes.
var ledgerScopedAttrTypes = []byte{
	dal.SubAttrVolume,
	dal.SubAttrMetadata,
	dal.SubAttrTransaction,
	dal.SubAttrReference,
	dal.SubAttrNumscriptVersion,
	dal.SubAttrNumscriptContent,
	dal.SubAttrPreparedQuery,
}

// DeleteLedgerData removes all per-ledger data from Pebble for the given ledger.
// This performs per-type range deletes on:
//   - Attributes zone (0xF1): one range delete per ledger-scoped attribute type
//   - Prepared queries: range delete for [zone][sub][ledgerID_BE_4B]
//   - Reversions: range delete for [zone][sub][ledgerID_BE_4B]
//   - Point deletes: mirror source head, mirror cursor, mirror status
//   - Point delete: pending ledger cleanup
//
// LedgerInfo and Boundaries are NOT deleted here — LedgerInfo is kept for "ledger deleted"
// responses, and Boundaries are handled separately via Attribute.Delete.
func deleteLedgerData(b *dal.WriteSession, ledgerID uint32) error {
	// Ledger ID as big-endian 4 bytes — this is the prefix for all
	// ledger-scoped canonical keys in the attributes zone.
	var ledgerPrefix [4]byte
	binary.BigEndian.PutUint32(ledgerPrefix[:], ledgerID)

	var ledgerPrefixUpper [4]byte
	binary.BigEndian.PutUint32(ledgerPrefixUpper[:], ledgerID+1)

	// Per-type range deletes: [0xF1][attrType][ledgerID_BE_4B] -> [0xF1][attrType][(ledgerID+1)_BE_4B]
	for _, attrType := range ledgerScopedAttrTypes {
		start := make([]byte, 2+4)
		start[0] = dal.ZoneAttributes
		start[1] = attrType
		copy(start[2:], ledgerPrefix[:])

		end := make([]byte, 2+4)
		end[0] = dal.ZoneAttributes
		end[1] = attrType
		copy(end[2:], ledgerPrefixUpper[:])

		if err := b.DeleteRange(start, end, nil); err != nil {
			return fmt.Errorf("deleting ledger attributes (type=0x%02x) for ledger %d: %w", attrType, ledgerID, err)
		}
	}

	// Range delete for per-ledger system keys scoped by [zone][sub][ledgerID_BE_4B].
	for _, sub := range []byte{dal.SubPLPreparedQuery, dal.SubPLReversions} {
		start := make([]byte, 2+4)
		start[0] = dal.ZonePerLedger
		start[1] = sub
		copy(start[2:], ledgerPrefix[:])

		end := make([]byte, 2+4)
		end[0] = dal.ZonePerLedger
		end[1] = sub
		copy(end[2:], ledgerPrefixUpper[:])

		if err := b.DeleteRange(start, end, nil); err != nil {
			return fmt.Errorf("deleting per-ledger keys sub=0x%02x for ledger %d: %w", sub, ledgerID, err)
		}
	}

	// Point deletes for per-ledger keys (keyed by [zone][sub][ledgerID_BE_4B]).
	for _, sub := range []byte{dal.SubPLMirrorSourceHead, dal.SubPLMirrorCursor, dal.SubPLMirrorStatus, dal.SubPLPendingCleanup} {
		key := make([]byte, 2+4)
		key[0] = dal.ZonePerLedger
		key[1] = sub
		copy(key[2:], ledgerPrefix[:])

		if err := b.DeleteKey(key); err != nil {
			return fmt.Errorf("deleting key sub=0x%02x for ledger %d: %w", sub, ledgerID, err)
		}
	}

	return nil
}

// SavePendingLedgerCleanup records a deferred ledger data cleanup keyed by ledger ID.
// The value is the sequence number of the DeleteLedger log.
func savePendingLedgerCleanup(b *dal.WriteSession, ledgerID uint32, deleteSequence uint64) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZonePerLedger, dal.SubPLPendingCleanup).
		PutLedgerID(ledgerID)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], deleteSequence)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("saving pending ledger cleanup for ledger %d: %w", ledgerID, err)
	}

	return nil
}

// DeletePendingLedgerCleanup removes a pending ledger cleanup entry after data has been purged.
func deletePendingLedgerCleanup(b *dal.WriteSession, ledgerID uint32) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZonePerLedger, dal.SubPLPendingCleanup).
		PutLedgerID(ledgerID)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("deleting pending ledger cleanup for ledger %d: %w", ledgerID, err)
	}

	return nil
}
