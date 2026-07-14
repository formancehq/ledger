package state

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger/v3/internal/pkg/bitset"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/proposalpb"
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

// appendAuditEntries appends audit entries to the batch.
//
// AuditEntry is the unit of the audit hash chain, so its persisted bytes
// are marshalled deterministically (MarshalDeterministicVT sorts the
// AuditFailure.context map keys). The hash chain itself is independent —
// it consumes BuildHashedHeaderPayload, which already canonicalises field
// order — but locking down on-disk byte order on the hash carrier itself
// is defence in depth: a cross-node byte compare on the audit stream
// now functions, and a future tool that reasons about the audit stream
// in bytes does not have to redo the canonicalisation. Log, AuditItem
// (no maps), and AppliedProposal stay on the non-deterministic
// SetProto path — they are not in the chain.
//
// SetProtoDeterministic reuses the session's marshal buffer, so each
// proposal pays one slice grow at most for the audit entry payload.
func appendAuditEntries(b *dal.WriteSession, entries ...*auditpb.AuditEntry) error {
	for _, entry := range entries {
		b.KeyBuilder.
			PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).
			PutUint64(entry.GetSequence())

		if err := b.SetProtoDeterministic(b.KeyBuilder.Consume(), entry); err != nil {
			return fmt.Errorf("inserting audit entry: %w", err)
		}
	}

	return nil
}

// appendAppliedProposal writes a Proposal side-effect record to the batch.
// Key format: [ZoneCold][SubColdAppliedProposal][sequence BE 8]. Called only
// on the success path; failed proposals do not emit one so the index builder
// can rely on a 1:1 mapping with AuditEntry on the success path.
//
// AppliedProposal carries a map (TransientVolumes) so two nodes will
// persist byte-divergent Cold-zone entries for the same proposal. This
// matches the existing Cold-zone contract: Log payloads (metadata maps,
// etc.) and AuditEntry (AuditFailure.context map) are also marshalled
// non-deterministically via SetProto. Cross-node authenticity goes
// through the audit hash chain, not byte equality of Cold-zone keys.
// Locking AppliedProposal alone behind MarshalDeterministicVT would
// create a two-speed standard without buying anything as long as the
// other Cold-zone writes stay non-deterministic.
func appendAppliedProposal(b *dal.WriteSession, applied *proposalpb.AppliedProposal) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZoneCold, dal.SubColdAppliedProposal).
		PutUint64(applied.GetSequence())

	if err := b.SetProto(b.KeyBuilder.Consume(), applied); err != nil {
		return fmt.Errorf("inserting applied proposal entry: %w", err)
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

// SaveChapterSchedule stores the chapter schedule cron expression in the batch.
func SaveChapterSchedule(b *dal.WriteSession, cron string) error {
	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobChapterSchedule}, []byte(cron))
	if err != nil {
		return fmt.Errorf("saving chapter schedule: %w", err)
	}

	return nil
}

// BatchDeleteChapterSchedule removes the chapter schedule from the batch.
func batchDeleteChapterSchedule(b *dal.WriteSession) error {
	err := b.DeleteKey([]byte{dal.ZoneGlobal, dal.SubGlobChapterSchedule})
	if err != nil {
		return fmt.Errorf("deleting chapter schedule: %w", err)
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
// Key: [0x03][0x01][ledgerName padded 64B][wordIndex BE 8 bytes] → [uint64 LE 8 bytes].
func SaveReversionWord(b *dal.WriteSession, ledgerName string, wordIndex uint64, value uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLReversions).
		PutLedgerNameFixed(ledgerName).
		PutUint64(wordIndex)

	if err := b.SetBytes(b.KeyBuilder.Consume(), bitset.MarshalWord(value)); err != nil {
		return fmt.Errorf("saving reversion word %d for ledger %q: %w", wordIndex, ledgerName, err)
	}

	return nil
}

// DeleteReversionsByLedger removes all reversion words for a ledger.
func deleteReversionsByLedger(b *dal.WriteSession, ledgerName string) error {
	start := buildLedgerScopedPrefix(dal.ZonePerLedger, dal.SubPLReversions, ledgerName)
	end := buildLedgerScopedPrefixSuccessor(dal.ZonePerLedger, dal.SubPLReversions, ledgerName)

	return b.DeleteRangeNoSync(start, end)
}

// buildLedgerScopedPrefix builds [zone][sub][ledgerName padded 64B] for
// range / point operations. The padded name is fixed-width so the comparer
// can split keys at offset 2+LedgerNameFixedSize without parsing.
func buildLedgerScopedPrefix(zone, sub byte, ledgerName string) []byte {
	out := make([]byte, 2+dal.LedgerNameFixedSize)
	out[0] = zone
	out[1] = sub

	copy(out[2:], ledgerName)

	return out
}

// buildLedgerScopedPrefixSuccessor returns the immediate successor of
// buildLedgerScopedPrefix(zone, sub, ledgerName) — used as the exclusive
// upper bound for DeleteRange. Increments the last byte of the padded
// name block; safe because the validation layer rejects names containing
// 0xFF bytes (printable ASCII only).
func buildLedgerScopedPrefixSuccessor(zone, sub byte, ledgerName string) []byte {
	out := buildLedgerScopedPrefix(zone, sub, ledgerName)
	out[len(out)-1]++

	return out
}

// SavePreparedQuery stores a prepared query in the batch under the given
// ledger. The ledger no longer lives on the PreparedQuery value itself —
// it is part of the key prefix and provided by the caller.
func SavePreparedQuery(b *dal.WriteSession, ledger string, pq *commonpb.PreparedQuery) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLPreparedQuery).
		PutLedgerNameFixed(ledger).
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

// StoreChapter marshals and writes a single chapter keyed by its ID.
func StoreChapter(b *dal.WriteSession, chapter *commonpb.Chapter) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZoneGlobal, dal.SubGlobChapters).
		PutUint64(chapter.GetId())

	err := b.SetProto(b.KeyBuilder.Consume(), chapter)
	if err != nil {
		return fmt.Errorf("storing chapter: %w", err)
	}

	return nil
}

// StoreNextChapterID writes the next chapter ID as 8-byte big-endian uint64.
func StoreNextChapterID(b *dal.WriteSession, id uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, id)

	err := b.SetBytes([]byte{dal.ZoneGlobal, dal.SubGlobNextChapterID}, value)
	if err != nil {
		return fmt.Errorf("storing next chapter ID: %w", err)
	}

	return nil
}

// SetMirrorSourceHead writes the latest known v2 source log count to the batch (Raft-replicated).
func SetMirrorSourceHead(b *dal.WriteSession, ledgerName string, count uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorSourceHead).
		PutLedgerNameFixed(ledgerName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], count)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("setting mirror source head: %w", err)
	}

	return nil
}

// SetMirrorCursor writes a per-ledger mirror cursor to the batch (Raft-replicated).
func SetMirrorCursor(b *dal.WriteSession, ledgerName string, cursor uint64) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorCursor).
		PutLedgerNameFixed(ledgerName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], cursor)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("setting mirror cursor: %w", err)
	}

	return nil
}

// SetMirrorStatus writes a per-ledger mirror sync error to the batch.
func SetMirrorStatus(b *dal.WriteSession, ledgerName string, syncErr *commonpb.MirrorSyncError) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorStatus).
		PutLedgerNameFixed(ledgerName)

	err := b.SetProto(b.KeyBuilder.Consume(), syncErr)
	if err != nil {
		return fmt.Errorf("setting mirror status: %w", err)
	}

	return nil
}

// ClearMirrorStatus removes a per-ledger mirror sync error from the batch.
func clearMirrorStatus(b *dal.WriteSession, ledgerName string) error {
	b.KeyBuilder.PutZonePrefix(dal.ZonePerLedger, dal.SubPLMirrorStatus).
		PutLedgerNameFixed(ledgerName)

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
// (format [ledger padded 64B]...). Used by DeleteLedgerData for per-type range
// deletes.
//
// SubAttrIndex carries ledger-scoped entries (keyed by
// IndexKey{LedgerName, Canonical} where LedgerName is encoded as the same
// 64-byte zero-padded block as the other ledger-scoped keys) alongside
// bucket-scoped audit entries (LedgerName == ""). The range delete here
// targets [SubAttrIndex][ledgerName padded 64B] → its byte-successor, so only
// the ledger-scoped half of the registry is purged — the bucket-scoped slot
// (empty LedgerName) lives under a distinct all-zero 64B prefix and is
// unreachable from a non-empty ledger name's range. processDeleteLedger also
// clears the cache-resident entries up-front for immediate visibility; without
// the Pebble-level range delete here, entries evicted from the cache before
// deletion would survive the purge (PR #453 review).
var ledgerScopedAttrTypes = []byte{
	dal.SubAttrVolume,
	dal.SubAttrMetadata,
	dal.SubAttrTransaction,
	dal.SubAttrReference,
	dal.SubAttrNumscriptVersion,
	dal.SubAttrNumscriptContent,
	dal.SubAttrPreparedQuery,
	dal.SubAttrIndex,
}

// DeleteLedgerData removes all per-ledger data from Pebble for the given ledger.
// This performs per-type range deletes on:
//   - Attributes zone (0xF1): one range delete per ledger-scoped attribute type
//   - Prepared queries: range delete for [zone][sub][ledgerName padded 64B]
//   - Reversions: range delete for [zone][sub][ledgerName padded 64B]
//   - Point deletes: mirror source head, mirror cursor, mirror status
//   - Point delete: pending ledger cleanup
//
// LedgerInfo and Boundaries are NOT deleted here — LedgerInfo is kept for "ledger deleted"
// responses, and Boundaries are handled separately via Attribute.Delete.
func deleteLedgerData(b *dal.WriteSession, ledgerName string) error {
	// Per-type range deletes: [0xF1][attrType][ledgerName padded] -> successor.
	for _, attrType := range ledgerScopedAttrTypes {
		start := buildLedgerScopedPrefix(dal.ZoneAttributes, attrType, ledgerName)
		end := buildLedgerScopedPrefixSuccessor(dal.ZoneAttributes, attrType, ledgerName)

		if err := b.DeleteRange(start, end, nil); err != nil {
			return fmt.Errorf("deleting ledger attributes (type=0x%02x) for ledger %q: %w", attrType, ledgerName, err)
		}
	}

	// Range delete for per-ledger system keys scoped by [zone][sub][ledgerName padded].
	for _, sub := range []byte{dal.SubPLPreparedQuery, dal.SubPLReversions} {
		start := buildLedgerScopedPrefix(dal.ZonePerLedger, sub, ledgerName)
		end := buildLedgerScopedPrefixSuccessor(dal.ZonePerLedger, sub, ledgerName)

		if err := b.DeleteRange(start, end, nil); err != nil {
			return fmt.Errorf("deleting per-ledger keys sub=0x%02x for ledger %q: %w", sub, ledgerName, err)
		}
	}

	// Point deletes for per-ledger keys (keyed by [zone][sub][ledgerName padded]).
	for _, sub := range []byte{dal.SubPLMirrorSourceHead, dal.SubPLMirrorCursor, dal.SubPLMirrorStatus, dal.SubPLPendingCleanup} {
		key := buildLedgerScopedPrefix(dal.ZonePerLedger, sub, ledgerName)

		if err := b.DeleteKey(key); err != nil {
			return fmt.Errorf("deleting key sub=0x%02x for ledger %q: %w", sub, ledgerName, err)
		}
	}

	return nil
}

// SavePendingLedgerCleanup records a deferred ledger data cleanup keyed by ledger name.
// The value is the sequence number of the DeleteLedger log.
func SavePendingLedgerCleanup(b *dal.WriteSession, ledgerName string, deleteSequence uint64) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZonePerLedger, dal.SubPLPendingCleanup).
		PutLedgerNameFixed(ledgerName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], deleteSequence)

	err := b.SetBytes(b.KeyBuilder.Consume(), buf[:])
	if err != nil {
		return fmt.Errorf("saving pending ledger cleanup for ledger %q: %w", ledgerName, err)
	}

	return nil
}

// DeletePendingLedgerCleanup removes a pending ledger cleanup entry after data has been purged.
func deletePendingLedgerCleanup(b *dal.WriteSession, ledgerName string) error {
	b.KeyBuilder.
		PutZonePrefix(dal.ZonePerLedger, dal.SubPLPendingCleanup).
		PutLedgerNameFixed(ledgerName)

	err := b.DeleteKey(b.KeyBuilder.Consume())
	if err != nil {
		return fmt.Errorf("deleting pending ledger cleanup for ledger %q: %w", ledgerName, err)
	}

	return nil
}
