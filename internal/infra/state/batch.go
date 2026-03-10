package state

import (
	"encoding/binary"
	"fmt"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/pkg/semver"
	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// AppendLogs appends system logs to the batch.
func AppendLogs(b *dal.Batch, logs ...*commonpb.Log) error {
	for _, log := range logs {
		b.KeyBuilder.
			PutByte(dal.KeyPrefixLog).
			PutUint64(log.GetSequence())

		err := b.SetProto(b.KeyBuilder.Build(), log)
		if err != nil {
			return fmt.Errorf("inserting system log: %w", err)
		}

		// Create idempotency index if present
		if log.GetIdempotency() != nil && log.GetIdempotency().GetKey() != "" {
			seqValue := make([]byte, 8)
			binary.BigEndian.PutUint64(seqValue, log.GetSequence())

			b.KeyBuilder.
				PutByte(dal.KeyPrefixIdempotency).
				PutString(log.GetIdempotency().GetKey())

			err := b.SetBytes(b.KeyBuilder.Build(), seqValue)
			if err != nil {
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
		PutString(info.GetName())

	err := b.SetProto(b.KeyBuilder.Build(), info)
	if err != nil {
		return fmt.Errorf("inserting ledger info: %w", err)
	}

	return nil
}

// AppendAuditEntries appends audit entries to the batch.
func AppendAuditEntries(b *dal.Batch, entries ...*auditpb.AuditEntry) error {
	for _, entry := range entries {
		b.KeyBuilder.
			PutByte(dal.KeyPrefixAudit).
			PutUint64(entry.GetSequence())

		err := b.SetProto(b.KeyBuilder.Build(), entry)
		if err != nil {
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

	err := b.SetBytes(b.KeyBuilder.Build(), value)
	if err != nil {
		return fmt.Errorf("saving signing key: %w", err)
	}

	return nil
}

// DeleteSigningKey removes a signing key from the batch.
func DeleteSigningKey(b *dal.Batch, keyID string) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixSigningKey).
		PutString(keyID)

	err := b.DeleteKey(b.KeyBuilder.Build())
	if err != nil {
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

	err := b.SetBytes([]byte{dal.KeyPrefixSigningConfig}, value)
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

	err := b.SetBytes([]byte{dal.KeyPrefixMaintenanceMode}, value)
	if err != nil {
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

	err := b.SetBytes([]byte{dal.KeyPrefixAuditConfig}, value)
	if err != nil {
		return fmt.Errorf("saving audit config: %w", err)
	}

	return nil
}

// SavePeriodSchedule stores the period schedule cron expression in the batch.
func SavePeriodSchedule(b *dal.Batch, cron string) error {
	err := b.SetBytes([]byte{dal.KeyPrefixPeriodSchedule}, []byte(cron))
	if err != nil {
		return fmt.Errorf("saving period schedule: %w", err)
	}

	return nil
}

// BatchDeletePeriodSchedule removes the period schedule from the batch.
func BatchDeletePeriodSchedule(b *dal.Batch) error {
	err := b.DeleteKey([]byte{dal.KeyPrefixPeriodSchedule})
	if err != nil {
		return fmt.Errorf("deleting period schedule: %w", err)
	}

	return nil
}

// SaveSinkConfig stores a per-sink configuration in the batch.
func SaveSinkConfig(b *dal.Batch, config *commonpb.SinkConfig) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixEventsConfig).
		PutString(config.GetName())

	err := b.SetProto(b.KeyBuilder.Build(), config)
	if err != nil {
		return fmt.Errorf("saving sink config: %w", err)
	}

	return nil
}

// DeleteSinkConfig removes a per-sink configuration from the batch.
func DeleteSinkConfig(b *dal.Batch, name string) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixEventsConfig).
		PutString(name)

	err := b.DeleteKey(b.KeyBuilder.Build())
	if err != nil {
		return fmt.Errorf("deleting sink config: %w", err)
	}

	return nil
}

// SavePreparedQuery stores a prepared query in the batch.
func SavePreparedQuery(b *dal.Batch, pq *commonpb.PreparedQuery) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixPreparedQuery).
		PutLedgerName(pq.GetLedger()).
		PutString(pq.GetName())

	err := b.SetProto(b.KeyBuilder.Build(), pq)
	if err != nil {
		return fmt.Errorf("saving prepared query: %w", err)
	}

	return nil
}

// DeletePreparedQuery removes a prepared query from the batch.
func DeletePreparedQuery(b *dal.Batch, ledger, name string) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixPreparedQuery).
		PutLedgerName(ledger).
		PutString(name)

	err := b.DeleteKey(b.KeyBuilder.Build())
	if err != nil {
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

	err := b.SetBytes(b.KeyBuilder.Build(), buf[:])
	if err != nil {
		return fmt.Errorf("setting sink cursor: %w", err)
	}

	return nil
}

// SetSinkStatus writes a per-sink status to the batch (Raft-replicated).
func SetSinkStatus(b *dal.Batch, status *commonpb.SinkStatus) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixSinkStatus).
		PutString(status.GetSinkName())

	err := b.SetProto(b.KeyBuilder.Build(), status)
	if err != nil {
		return fmt.Errorf("setting sink status: %w", err)
	}

	return nil
}

// ClearSinkStatus removes a per-sink status from the batch.
func ClearSinkStatus(b *dal.Batch, sinkName string) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixSinkStatus).
		PutString(sinkName)

	err := b.DeleteKey(b.KeyBuilder.Build())
	if err != nil {
		return fmt.Errorf("clearing sink status: %w", err)
	}

	return nil
}

// StorePeriod marshals and writes a single period keyed by its ID.
func StorePeriod(b *dal.Batch, period *commonpb.Period) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixPeriods).
		PutUint64(period.GetId())

	err := b.SetProto(b.KeyBuilder.Build(), period)
	if err != nil {
		return fmt.Errorf("storing period: %w", err)
	}

	return nil
}

// StoreNextPeriodID writes the next period ID as 8-byte big-endian uint64.
func StoreNextPeriodID(b *dal.Batch, id uint64) error {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, id)

	err := b.SetBytes([]byte{dal.KeyPrefixNextPeriodID}, value)
	if err != nil {
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

	err := b.SetBytes(b.KeyBuilder.Build(), buf[:])
	if err != nil {
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

	err := b.SetBytes(b.KeyBuilder.Build(), buf[:])
	if err != nil {
		return fmt.Errorf("setting mirror cursor: %w", err)
	}

	return nil
}

// SetMirrorStatus writes a per-ledger mirror sync error to the batch.
func SetMirrorStatus(b *dal.Batch, ledgerName string, syncErr *commonpb.MirrorSyncError) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixMirrorStatus).
		PutString(ledgerName)

	err := b.SetProto(b.KeyBuilder.Build(), syncErr)
	if err != nil {
		return fmt.Errorf("setting mirror status: %w", err)
	}

	return nil
}

// ClearMirrorStatus removes a per-ledger mirror sync error from the batch.
func ClearMirrorStatus(b *dal.Batch, ledgerName string) error {
	b.KeyBuilder.PutByte(dal.KeyPrefixMirrorStatus).
		PutString(ledgerName)

	err := b.DeleteKey(b.KeyBuilder.Build())
	if err != nil {
		return fmt.Errorf("clearing mirror status: %w", err)
	}

	return nil
}

// WriteSeqToRaftIndex writes a mapping from the first log sequence produced by
// a raft entry to the raft index. Used by the index builder to correlate bbolt
// progress (sequence-based) with Pebble attribute versions (raft-index-based).
// Key: [0x04][firstSeq BE 8B]  Value: [raftIndex BE 8B].
func WriteSeqToRaftIndex(b *dal.Batch, firstSeq, raftIndex uint64) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixSeqToRaftIndex).
		PutUint64(firstSeq)

	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], raftIndex)

	return b.SetBytes(b.KeyBuilder.Build(), buf[:])
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

// SaveNumscript stores a versioned numscript entry and updates the latest version pointer.
// Semver versions are encoded as [prefix][name]\x00\x00[major_u32BE][minor_u32BE][patch_u32BE].
// The "latest" slot is encoded as [prefix][name]\x00\x01.
func SaveNumscript(b *dal.Batch, info *commonpb.NumscriptInfo) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixNumscript).
		PutString(info.GetName()).
		PutByte(0x00)

	if info.GetVersion() == "latest" {
		b.KeyBuilder.PutByte(domain.NumscriptVersionTagLatest)
	} else {
		sv, err := semver.Parse(info.GetVersion())
		if err != nil {
			return fmt.Errorf("saving numscript %q: %w", info.GetName(), err)
		}

		b.KeyBuilder.
			PutByte(domain.NumscriptVersionTagSemver).
			PutUint32(sv.Major).
			PutUint32(sv.Minor).
			PutUint32(sv.Patch)
	}

	err := b.SetProto(b.KeyBuilder.Build(), info)
	if err != nil {
		return fmt.Errorf("saving numscript %q v%s: %w", info.GetName(), info.GetVersion(), err)
	}

	// Update latest version pointer: [prefix][name] -> version string bytes
	b.KeyBuilder.
		PutByte(dal.KeyPrefixNumscriptLatest).
		PutString(info.GetName())

	err = b.SetBytes(b.KeyBuilder.Build(), []byte(info.GetVersion()))
	if err != nil {
		return fmt.Errorf("saving numscript latest version for %q: %w", info.GetName(), err)
	}

	return nil
}

// ClearNumscriptLatestVersion soft-deletes a numscript by writing an empty version pointer.
// The version entries remain in Pebble and are still accessible by explicit version.
func ClearNumscriptLatestVersion(b *dal.Batch, name string) error {
	b.KeyBuilder.
		PutByte(dal.KeyPrefixNumscriptLatest).
		PutString(name)

	err := b.SetBytes(b.KeyBuilder.Build(), nil)
	if err != nil {
		return fmt.Errorf("clearing numscript latest version for %q: %w", name, err)
	}

	return nil
}
