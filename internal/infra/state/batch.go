package state

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/semver"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// AppendLogs appends system logs to the batch.
func AppendLogs(b *dal.Batch, logs ...*commonpb.Log) error {
	for _, log := range logs {
		b.KeyBuilder.
			PutByte(dal.KeyPrefixLog).
			PutUInt64(log.Sequence)

		if err := b.SetProto(b.KeyBuilder.Build(), log); err != nil {
			return fmt.Errorf("inserting system log: %w", err)
		}

		// Create idempotency index if present
		if log.Idempotency != nil && log.Idempotency.Key != "" {
			seqValue := make([]byte, 8)
			binary.BigEndian.PutUint64(seqValue, log.Sequence)

			b.KeyBuilder.
				PutByte(dal.KeyPrefixIdempotency).
				PutString(log.Idempotency.Key)
			if err := b.SetBytes(b.KeyBuilder.Build(), seqValue); err != nil {
				return fmt.Errorf("inserting idempotency index: %w", err)
			}
		}
	}

	return nil
}

// SaveLedger saves or updates a ledger in the store.
func SaveLedger(b *dal.Batch, info *commonpb.LedgerInfo) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixLedgerInfo).
		PutString(info.Name)

	if err := b.SetProto(b.KeyBuilder.Build(), info); err != nil {
		return fmt.Errorf("inserting ledger info: %w", err)
	}

	return nil
}

// StoreTransactionUpdate stores a transaction update (init, revert, add/delete metadata).
// Key: [KeyPrefixTransactionUpdate][name]\x00[transactionID(8)][byLog(8)] -> TransactionUpdate
func StoreTransactionUpdate(b *dal.Batch, key domain.TransactionKey, update *commonpb.TransactionUpdate) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixTransactionUpdate).
		PutLedgerName(key.Ledger).
		PutUInt64(key.ID).
		PutUInt64(update.ByLog)

	if err := b.SetProto(b.KeyBuilder.Build(), update); err != nil {
		return fmt.Errorf("storing transaction update: %w", err)
	}

	return nil
}

// AppendAuditEntries appends audit entries to the batch.
func AppendAuditEntries(b *dal.Batch, entries ...*auditpb.AuditEntry) error {
	for _, entry := range entries {
		b.KeyBuilder.
			PutByte(dal.KeyPrefixAudit).
			PutUInt64(entry.Sequence)

		if err := b.SetProto(b.KeyBuilder.Build(), entry); err != nil {
			return fmt.Errorf("inserting audit entry: %w", err)
		}
	}

	return nil
}

// SaveSigningKey stores an Ed25519 public key in the batch.
// The value format is [publicKey (32 bytes)][parentKeyID (UTF-8 variable)].
// Backward-compatible: existing 32-byte values are treated as root keys (no parent).
func SaveSigningKey(b *dal.Batch, keyID string, publicKey []byte, parentKeyID string) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixSigningKey).
		PutString(keyID)

	value := publicKey
	if parentKeyID != "" {
		value = make([]byte, len(publicKey)+len(parentKeyID))
		copy(value, publicKey)
		copy(value[len(publicKey):], parentKeyID)
	}

	if err := b.SetBytes(b.KeyBuilder.Build(), value); err != nil {
		return fmt.Errorf("saving signing key: %w", err)
	}
	return nil
}

// DeleteSigningKey removes a signing key from the batch.
func DeleteSigningKey(b *dal.Batch, keyID string) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixSigningKey).
		PutString(keyID)

	if err := b.DeleteKey(b.KeyBuilder.Build()); err != nil {
		return fmt.Errorf("deleting signing key: %w", err)
	}
	return nil
}

// DeleteAllSigningKeys removes all signing keys from the batch using a range delete.
func DeleteAllSigningKeys(b *dal.Batch) error {
	return b.DeleteRangeNoSync(
		[]byte{dal.KeyPrefixSigningKey},
		[]byte{dal.KeyPrefixSigningKey + 1},
	)
}

// SaveSigningConfig stores the require-signatures flag in the batch.
func SaveSigningConfig(b *dal.Batch, requireSignatures bool) error {
	value := []byte{0x00}
	if requireSignatures {
		value[0] = 0x01
	}

	if err := b.SetBytes([]byte{dal.KeyPrefixSigningConfig}, value); err != nil {
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

	if err := b.SetBytes([]byte{dal.KeyPrefixMaintenanceMode}, value); err != nil {
		return fmt.Errorf("saving maintenance mode: %w", err)
	}
	return nil
}

// SaveAuditConfig stores the audit enabled flag in the batch.
func SaveAuditConfig(b *dal.Batch, enabled bool) error {
	value := []byte{0x00}
	if enabled {
		value[0] = 0x01
	}

	if err := b.SetBytes([]byte{dal.KeyPrefixAuditConfig}, value); err != nil {
		return fmt.Errorf("saving audit config: %w", err)
	}
	return nil
}

// SavePeriodSchedule stores the period schedule cron expression in the batch.
func SavePeriodSchedule(b *dal.Batch, cron string) error {
	if err := b.SetBytes([]byte{dal.KeyPrefixPeriodSchedule}, []byte(cron)); err != nil {
		return fmt.Errorf("saving period schedule: %w", err)
	}
	return nil
}

// BatchDeletePeriodSchedule removes the period schedule from the batch.
func BatchDeletePeriodSchedule(b *dal.Batch) error {
	if err := b.DeleteKey([]byte{dal.KeyPrefixPeriodSchedule}); err != nil {
		return fmt.Errorf("deleting period schedule: %w", err)
	}
	return nil
}

// SaveSinkConfig stores a per-sink configuration in the batch.
func SaveSinkConfig(b *dal.Batch, config *commonpb.SinkConfig) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixEventsConfig).
		PutString(config.Name)

	if err := b.SetProto(b.KeyBuilder.Build(), config); err != nil {
		return fmt.Errorf("saving sink config: %w", err)
	}
	return nil
}

// DeleteSinkConfig removes a per-sink configuration from the batch.
func DeleteSinkConfig(b *dal.Batch, name string) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixEventsConfig).
		PutString(name)

	if err := b.DeleteKey(b.KeyBuilder.Build()); err != nil {
		return fmt.Errorf("deleting sink config: %w", err)
	}
	return nil
}

// SavePreparedQuery stores a prepared query in the batch.
func SavePreparedQuery(b *dal.Batch, pq *commonpb.PreparedQuery) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixPreparedQuery).
		PutLedgerName(pq.Ledger).
		PutString(pq.Name)

	if err := b.SetProto(b.KeyBuilder.Build(), pq); err != nil {
		return fmt.Errorf("saving prepared query: %w", err)
	}
	return nil
}

// DeletePreparedQuery removes a prepared query from the batch.
func DeletePreparedQuery(b *dal.Batch, ledger, name string) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixPreparedQuery).
		PutLedgerName(ledger).
		PutString(name)

	if err := b.DeleteKey(b.KeyBuilder.Build()); err != nil {
		return fmt.Errorf("deleting prepared query: %w", err)
	}
	return nil
}

// SetSinkCursor writes a per-sink events cursor to the batch (Raft-replicated).
func SetSinkCursor(b *dal.Batch, sinkName string, sequence uint64) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixSinkCursor).
		PutString(sinkName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], sequence)
	if err := b.SetBytes(b.KeyBuilder.Build(), buf[:]); err != nil {
		return fmt.Errorf("setting sink cursor: %w", err)
	}
	return nil
}

// SetSinkStatus writes a per-sink status to the batch (Raft-replicated).
func SetSinkStatus(b *dal.Batch, status *commonpb.SinkStatus) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixSinkStatus).
		PutString(status.SinkName)

	if err := b.SetProto(b.KeyBuilder.Build(), status); err != nil {
		return fmt.Errorf("setting sink status: %w", err)
	}
	return nil
}

// ClearSinkStatus removes a per-sink status from the batch.
func ClearSinkStatus(b *dal.Batch, sinkName string) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixSinkStatus).
		PutString(sinkName)

	if err := b.DeleteKey(b.KeyBuilder.Build()); err != nil {
		return fmt.Errorf("clearing sink status: %w", err)
	}
	return nil
}

// StorePeriod marshals and writes a single period keyed by its ID.
func StorePeriod(b *dal.Batch, period *commonpb.Period) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixPeriods).
		PutUInt64(period.Id)

	if err := b.SetProto(b.KeyBuilder.Build(), period); err != nil {
		return fmt.Errorf("storing period: %w", err)
	}
	return nil
}

// StoreNextPeriodID writes the next period ID as 8-byte big-endian uint64.
func StoreNextPeriodID(b *dal.Batch, id uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, id)
	if err := b.SetBytes([]byte{dal.KeyPrefixNextPeriodID}, value); err != nil {
		return fmt.Errorf("storing next period ID: %w", err)
	}
	return nil
}

// SetMirrorSourceHead writes the latest known v2 source log count to the batch (Raft-replicated).
func SetMirrorSourceHead(b *dal.Batch, ledgerName string, count uint64) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixMirrorSourceHead).
		PutString(ledgerName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], count)
	if err := b.SetBytes(b.KeyBuilder.Build(), buf[:]); err != nil {
		return fmt.Errorf("setting mirror source head: %w", err)
	}
	return nil
}

// SetMirrorCursor writes a per-ledger mirror cursor to the batch (Raft-replicated).
func SetMirrorCursor(b *dal.Batch, ledgerName string, cursor uint64) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixMirrorCursor).
		PutString(ledgerName)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], cursor)
	if err := b.SetBytes(b.KeyBuilder.Build(), buf[:]); err != nil {
		return fmt.Errorf("setting mirror cursor: %w", err)
	}
	return nil
}

// SetMirrorStatus writes a per-ledger mirror sync error to the batch.
func SetMirrorStatus(b *dal.Batch, ledgerName string, syncErr *commonpb.MirrorSyncError) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixMirrorStatus).
		PutString(ledgerName)

	if err := b.SetProto(b.KeyBuilder.Build(), syncErr); err != nil {
		return fmt.Errorf("setting mirror status: %w", err)
	}
	return nil
}

// ClearMirrorStatus removes a per-ledger mirror sync error from the batch.
func ClearMirrorStatus(b *dal.Batch, ledgerName string) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixMirrorStatus).
		PutString(ledgerName)

	if err := b.DeleteKey(b.KeyBuilder.Build()); err != nil {
		return fmt.Errorf("clearing mirror status: %w", err)
	}
	return nil
}

// SetAppliedIndex writes the last applied Raft index to the batch.
func SetAppliedIndex(b *dal.Batch, index uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, index)
	return b.SetBytes([]byte{dal.KeyPrefixLastAppliedIndex}, value)
}

// SetLastAppliedTimestamp writes the last applied HLC timestamp to the batch.
func SetLastAppliedTimestamp(b *dal.Batch, timestamp uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, timestamp)
	return b.SetBytes([]byte{dal.KeyPrefixLastAppliedTimestamp}, value)
}

// PurgeTransactionUpdates deletes all transaction update entries whose byLog
// field falls in [startSeq, closeSeq]. The key format is
// [prefix(1)][name]\x00[txID(8)][byLog(8)], so byLog is the last 8 bytes.
func PurgeTransactionUpdates(b *dal.Batch, startSeq, closeSeq uint64) error {
	iter, err := b.NewIter(&pebble.IterOptions{
		LowerBound: []byte{dal.KeyPrefixTransactionUpdate},
		UpperBound: []byte{dal.KeyPrefixTransactionUpdate + 1},
	})
	if err != nil {
		return fmt.Errorf("creating iterator for transaction update purge: %w", err)
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		// Key must be at least prefix(1) + \x00(1) + txID(8) + byLog(8) = 18 bytes
		if len(key) < 18 {
			continue
		}
		byLog := binary.BigEndian.Uint64(key[len(key)-8:])
		if byLog < startSeq || byLog > closeSeq {
			continue
		}
		if err := b.DeleteKey(key); err != nil {
			return fmt.Errorf("deleting transaction update: %w", err)
		}
	}

	return iter.Error()
}

// SaveNumscript stores a versioned numscript entry and updates the latest version pointer.
// Semver versions are encoded as [prefix][name]\x00\x00[major_u32BE][minor_u32BE][patch_u32BE].
// The "latest" slot is encoded as [prefix][name]\x00\x01.
func SaveNumscript(b *dal.Batch, info *commonpb.NumscriptInfo) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixNumscript).
		PutString(info.Name).
		PutByte(0x00)

	if info.Version == "latest" {
		b.KeyBuilder.PutByte(domain.NumscriptVersionTagLatest)
	} else {
		sv, err := semver.Parse(info.Version)
		if err != nil {
			return fmt.Errorf("saving numscript %q: %w", info.Name, err)
		}
		b.KeyBuilder.
			PutByte(domain.NumscriptVersionTagSemver).
			PutUInt32(sv.Major).
			PutUInt32(sv.Minor).
			PutUInt32(sv.Patch)
	}

	if err := b.SetProto(b.KeyBuilder.Build(), info); err != nil {
		return fmt.Errorf("saving numscript %q v%s: %w", info.Name, info.Version, err)
	}

	// Update latest version pointer: [prefix][name] -> version string bytes
	b.KeyBuilder.
		PutByte(dal.KeyPrefixNumscriptLatest).
		PutString(info.Name)
	if err := b.SetBytes(b.KeyBuilder.Build(), []byte(info.Version)); err != nil {
		return fmt.Errorf("saving numscript latest version for %q: %w", info.Name, err)
	}
	return nil
}

// ClearNumscriptLatestVersion soft-deletes a numscript by writing an empty version pointer.
// The version entries remain in Pebble and are still accessible by explicit version.
func ClearNumscriptLatestVersion(b *dal.Batch, name string) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixNumscriptLatest).
		PutString(name)
	if err := b.SetBytes(b.KeyBuilder.Build(), nil); err != nil {
		return fmt.Errorf("clearing numscript latest version for %q: %w", name, err)
	}
	return nil
}
