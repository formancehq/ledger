package state

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/bitset"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// AppendLogs appends system logs to the batch.
func AppendLogs(b *dal.Batch, logs ...*commonpb.Log) error {
	for _, log := range logs {
		b.KeyBuilder.
			PutZonePrefix(dal.ZoneCold, dal.SubColdLog).
			PutUint64(log.GetSequence())

		err := b.SetProto(b.KeyBuilder.Consume(), log)
		if err != nil {
			return fmt.Errorf("inserting system log: %w", err)
		}
	}

	return nil
}

// SaveLedger saves or updates a ledger in the store.
func SaveLedger(b *dal.Batch, info *commonpb.LedgerInfo) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZoneGlobal, dal.SubGlobLedgerInfo).
		PutLedgerName(info.GetName())

	err := b.SetProto(b.KeyBuilder.Consume(), info)
	if err != nil {
		return fmt.Errorf("inserting ledger info: %w", err)
	}

	return nil
}

// AppendAuditEntries appends audit entries to the batch.
func AppendAuditEntries(b *dal.Batch, entries ...*auditpb.AuditEntry) error {
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
func AppendAuditItems(b *dal.Batch, auditSequence uint64, items ...*auditpb.AuditItem) error {
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
func SaveSigningKey(b *dal.Batch, keyID string, publicKey []byte, parentKeyID string) error {
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
func DeleteSigningKey(b *dal.Batch, keyID string) error {
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
func SaveSigningConfig(b *dal.Batch, requireSignatures bool) error {
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
func SaveMaintenanceMode(b *dal.Batch, enabled bool) error {
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
func SaveClusterState(b *dal.Batch, state *commonpb.PersistedClusterState) error {
	return b.SetProto([]byte{dal.ZoneGlobal, dal.SubGlobClusterConfig}, state)
}

// SavePeriodSchedule stores the period schedule cron expression in the batch.
func SavePeriodSchedule(b *dal.Batch, cron string) error {
	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobPeriodSchedule}, []byte(cron))
	if err != nil {
		return fmt.Errorf("saving period schedule: %w", err)
	}

	return nil
}

// BatchDeletePeriodSchedule removes the period schedule from the batch.
func BatchDeletePeriodSchedule(b *dal.Batch) error {
	err := b.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobPeriodSchedule})
	if err != nil {
		return fmt.Errorf("deleting period schedule: %w", err)
	}

	return nil
}

// SaveQueryCheckpointSchedule stores the query checkpoint schedule cron expression in the batch.
func SaveQueryCheckpointSchedule(b *dal.Batch, cron string) error {
	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobQueryCheckpointSchedule}, []byte(cron))
	if err != nil {
		return fmt.Errorf("saving query checkpoint schedule: %w", err)
	}

	return nil
}

// BatchDeleteQueryCheckpointSchedule removes the query checkpoint schedule from the batch.
func BatchDeleteQueryCheckpointSchedule(b *dal.Batch) error {
	err := b.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobQueryCheckpointSchedule})
	if err != nil {
		return fmt.Errorf("deleting query checkpoint schedule: %w", err)
	}

	return nil
}

// SaveReversionWord writes a single bitset word for a ledger.
// Key: [0x03][0x01][ledger\x00][wordIndex BE 8 bytes] → [uint64 LE 8 bytes].
func SaveReversionWord(b *dal.Batch, ledger string, wordIndex uint64, value uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLReversions).
		PutLedgerName(ledger).
		PutUint64(wordIndex)

	if err := b.SetBytes(b.KeyBuilder.Consume(), bitset.MarshalWord(value)); err != nil {
		return fmt.Errorf("saving reversion word %d for %s: %w", wordIndex, ledger, err)
	}

	return nil
}

// DeleteReversionsByLedger removes all reversion words for a ledger.
func DeleteReversionsByLedger(b *dal.Batch, ledger string) error {
	prefix := make([]byte, 2+len(ledger)+1)
	prefix[0] = dal.ZonePerLedger
	prefix[1] = dal.SubPLReversions
	copy(prefix[2:], ledger)
	prefix[len(prefix)-1] = 0x00

	end := make([]byte, len(prefix))
	copy(end, prefix)
	end[len(end)-1]++

	return b.DeleteRangeNoSync(prefix, end)
}

// SavePreparedQuery stores a prepared query in the batch.
func SavePreparedQuery(b *dal.Batch, pq *commonpb.PreparedQuery) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLPreparedQuery).
		PutLedgerName(pq.GetLedger()).
		PutString(pq.GetName())

	err := b.SetProto(b.KeyBuilder.Consume(), pq)
	if err != nil {
		return fmt.Errorf("saving prepared query: %w", err)
	}

	return nil
}

// DeletePreparedQuery removes a prepared query from the batch.
func DeletePreparedQuery(b *dal.Batch, ledger, name string) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLPreparedQuery).
		PutLedgerName(ledger).
		PutString(name)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("deleting prepared query: %w", err)
	}

	return nil
}

// SaveQueryCheckpoint stores a query checkpoint state in the batch (Raft-replicated).
func SaveQueryCheckpoint(b *dal.Batch, cp *raftcmdpb.QueryCheckpointState) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobQueryCheckpoint).
		PutUint64(cp.GetCheckpointId())

	err := b.SetProto(b.KeyBuilder.Consume(), cp)
	if err != nil {
		return fmt.Errorf("saving query checkpoint: %w", err)
	}

	return nil
}

// DeleteQueryCheckpointFromBatch removes a query checkpoint from the batch (Raft-replicated).
func DeleteQueryCheckpointFromBatch(b *dal.Batch, checkpointID uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobQueryCheckpoint).
		PutUint64(checkpointID)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("deleting query checkpoint: %w", err)
	}

	return nil
}

// StoreNextQueryCheckpointID writes the next query checkpoint ID as 8-byte big-endian uint64.
func StoreNextQueryCheckpointID(b *dal.Batch, id uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, id)

	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobNextQueryCheckpointID}, value)
	if err != nil {
		return fmt.Errorf("storing next query checkpoint ID: %w", err)
	}

	return nil
}

// SetSinkCursor writes a per-sink events cursor to the batch (Raft-replicated).
func SetSinkCursor(b *dal.Batch, sinkName string, sequence uint64) error {
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
func SetSinkStatus(b *dal.Batch, status *commonpb.SinkStatus) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkStatus).
		PutString(status.GetSinkName())

	err := b.SetProto(b.KeyBuilder.Consume(), status)
	if err != nil {
		return fmt.Errorf("setting sink status: %w", err)
	}

	return nil
}

// ClearSinkStatus removes a per-sink status from the batch.
func ClearSinkStatus(b *dal.Batch, sinkName string) error {
	b.KeyBuilder.PutZonePrefix(dal.ZoneGlobal, dal.SubGlobSinkStatus).
		PutString(sinkName)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("clearing sink status: %w", err)
	}

	return nil
}

// StorePeriod marshals and writes a single period keyed by its ID.
func StorePeriod(b *dal.Batch, period *commonpb.Period) error {
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
func StoreNextPeriodID(b *dal.Batch, id uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, id)

	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobNextPeriodID}, value)
	if err != nil {
		return fmt.Errorf("storing next period ID: %w", err)
	}

	return nil
}

// SetMirrorSourceHead writes the latest known v2 source log count to the batch (Raft-replicated).
func SetMirrorSourceHead(b *dal.Batch, ledgerName string, count uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorSourceHead).
		PutLedgerName(ledgerName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], count)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("setting mirror source head: %w", err)
	}

	return nil
}

// SetMirrorCursor writes a per-ledger mirror cursor to the batch (Raft-replicated).
func SetMirrorCursor(b *dal.Batch, ledgerName string, cursor uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorCursor).
		PutLedgerName(ledgerName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], cursor)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("setting mirror cursor: %w", err)
	}

	return nil
}

// SetMirrorStatus writes a per-ledger mirror sync error to the batch.
func SetMirrorStatus(b *dal.Batch, ledgerName string, syncErr *commonpb.MirrorSyncError) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorStatus).
		PutLedgerName(ledgerName)

	err := b.SetProto(b.KeyBuilder.Consume(), syncErr)
	if err != nil {
		return fmt.Errorf("setting mirror status: %w", err)
	}

	return nil
}

// ClearMirrorStatus removes a per-ledger mirror sync error from the batch.
func ClearMirrorStatus(b *dal.Batch, ledgerName string) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorStatus).
		PutLedgerName(ledgerName)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("clearing mirror status: %w", err)
	}

	return nil
}

// SetAppliedIndex writes the last applied Raft index to the batch.
func SetAppliedIndex(b *dal.Batch, index uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, index)

	return b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobLastAppliedIndex}, value)
}

// SetLastAppliedTimestamp writes the last applied HLC timestamp to the batch.
func SetLastAppliedTimestamp(b *dal.Batch, timestamp uint64) error {
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
//   - Prepared queries (0xE0): range delete for [0xE0][ledger\x00]
//   - Reversions (0xE5): range delete for [0xE5][ledger\x00]
//   - Point deletes: mirror source head (0xEB), mirror cursor (0xEC), mirror status (0xED)
//   - Point delete: pending ledger cleanup (0xE1)
//
// LedgerInfo and Boundaries are NOT deleted here — LedgerInfo is kept for "ledger deleted"
// responses, and Boundaries are handled separately via Attribute.Delete.
func DeleteLedgerData(b *dal.Batch, ledgerName string) error {
	// Ledger name followed by null separator — this is the prefix for all
	// ledger-scoped canonical keys in the attributes zone.
	ledgerPrefix := append([]byte(ledgerName), 0x00)
	ledgerPrefixUpper := attributes.IncrementBytes(ledgerPrefix)

	// Per-type range deletes: [0xF1][attrType][ledger\x00] -> [0xF1][attrType][ledger\x01]
	for _, attrType := range ledgerScopedAttrTypes {
		start := make([]byte, 2+len(ledgerPrefix))
		start[0] = dal.ZoneAttributes
		start[1] = attrType
		copy(start[2:], ledgerPrefix)

		end := make([]byte, 2+len(ledgerPrefixUpper))
		end[0] = dal.ZoneAttributes
		end[1] = attrType
		copy(end[2:], ledgerPrefixUpper)

		if err := b.DeleteRange(start, end, nil); err != nil {
			return fmt.Errorf("deleting ledger attributes (type=%c) for %q: %w", attrType, ledgerName, err)
		}
	}

	// Range delete for per-ledger system keys scoped by [zone][sub][ledger\x00].
	for _, sub := range []byte{dal.SubPLPreparedQuery, dal.SubPLReversions} {
		start := make([]byte, 2+len(ledgerPrefix))
		start[0] = dal.ZonePerLedger
		start[1] = sub
		copy(start[2:], ledgerPrefix)

		end := make([]byte, 2+len(ledgerPrefixUpper))
		end[0] = dal.ZonePerLedger
		end[1] = sub
		copy(end[2:], ledgerPrefixUpper)

		if err := b.DeleteRange(start, end, nil); err != nil {
			return fmt.Errorf("deleting per-ledger keys sub=0x%02x for %q: %w", sub, ledgerName, err)
		}
	}

	// Point deletes for per-ledger keys (keyed by [zone][sub][ledgerName\x00]).
	for _, sub := range []byte{dal.SubPLMirrorSourceHead, dal.SubPLMirrorCursor, dal.SubPLMirrorStatus, dal.SubPLPendingCleanup} {
		key := make([]byte, 2+len(ledgerName)+1)
		key[0] = dal.ZonePerLedger
		key[1] = sub
		copy(key[2:], ledgerName)
		key[len(key)-1] = 0x00

		if err := b.DeleteKey(key); err != nil {
			return fmt.Errorf("deleting key sub=0x%02x for ledger %q: %w", sub, ledgerName, err)
		}
	}

	return nil
}

// SavePendingLedgerCleanup records a deferred ledger data cleanup keyed by ledger name.
// The value is the sequence number of the DeleteLedger log.
func SavePendingLedgerCleanup(b *dal.Batch, ledgerName string, deleteSequence uint64) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZonePerLedger, dal.SubPLPendingCleanup).
		PutLedgerName(ledgerName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], deleteSequence)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("saving pending ledger cleanup for %q: %w", ledgerName, err)
	}

	return nil
}

// DeletePendingLedgerCleanup removes a pending ledger cleanup entry after data has been purged.
func DeletePendingLedgerCleanup(b *dal.Batch, ledgerName string) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZonePerLedger, dal.SubPLPendingCleanup).
		PutLedgerName(ledgerName)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("deleting pending ledger cleanup for %q: %w", ledgerName, err)
	}

	return nil
}
