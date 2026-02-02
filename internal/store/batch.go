package store

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// Batch provides atomic operations on the store using a pebble.Batch with NoSync.
// All operations are buffered until Commit is called.
// Cancel must be called if the batch is not committed to release resources.
type Batch struct {
	store            *Store
	batch            *pebble.Batch
	lastAppliedIndex uint64
	keyBuffer        *bytes.Buffer
	protoBuffer      []byte
	committed        bool
	marshalOptions   proto.MarshalOptions
}

// NewBatch creates a new Batch for atomic operations.
func (s *Store) NewBatch(lastAppliedIndex uint64) *Batch {
	return &Batch{
		store:            s,
		batch:            s.db.NewBatch(),
		lastAppliedIndex: lastAppliedIndex,
		keyBuffer:        bytes.NewBuffer(make([]byte, 0, 1024)),
		protoBuffer:      make([]byte, 0, 1024),
	}
}

// AppendLogs appends system logs to the batch.
func (b *Batch) AppendLogs(logs ...*commonpb.Log) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	for _, log := range logs {
		// Store the system log by sequence
		logBinary, err := b.marshalOptions.MarshalAppend(b.protoBuffer, log)
		if err != nil {
			return fmt.Errorf("marshaling system log to protobuf: %w", err)
		}

		writeByte(b.keyBuffer, keyPrefixLog)
		writeUInt64(b.keyBuffer, log.Sequence)

		if err := setOnBatch(b.batch, b.keyBuffer, logBinary); err != nil {
			return fmt.Errorf("inserting system log: %w", err)
		}

		// Create idempotency index if present
		if log.Idempotency != nil && log.Idempotency.Key != "" {
			seqValue := make([]byte, 8)
			binary.BigEndian.PutUint64(seqValue, log.Sequence)
			writeByte(b.keyBuffer, keyPrefixIdempotency)
			writeString(b.keyBuffer, log.Idempotency.Key)
			if err := setOnBatch(b.batch, b.keyBuffer, seqValue); err != nil {
				return fmt.Errorf("inserting idempotency index: %w", err)
			}
		}
	}

	return nil
}

// SaveLedger saves or updates a ledger in the store.
func (b *Batch) SaveLedger(info *commonpb.LedgerInfo) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	// Marshal LedgerInfo to protobuf
	infoBinary, err := proto.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshaling ledger info to protobuf: %w", err)
	}

	// Store with key: prefix + ledger ID (4 bytes big-endian)
	writeByte(b.keyBuffer, keyPrefixLedgerInfo)
	if err := binary.Write(b.keyBuffer, binary.BigEndian, info.Id); err != nil {
		return fmt.Errorf("writing ledger ID: %w", err)
	}

	if err := setOnBatch(b.batch, b.keyBuffer, infoBinary); err != nil {
		return fmt.Errorf("inserting ledger info: %w", err)
	}

	return nil
}

// AppendBalanceDiff appends a balance diff for an account/asset pair.
func (b *Batch) AppendBalanceDiff(diff BalanceDiff) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, diff.LedgerID)
	writeByte(b.keyBuffer, keyPrefixBalanceDiff)
	writeString(b.keyBuffer, diff.Account)
	writeString(b.keyBuffer, diff.Asset)
	writeUInt64(b.keyBuffer, diff.RaftIndex)

	bigIntData, err := b.marshalOptions.MarshalAppend(b.protoBuffer, diff.Diff)
	if err != nil {
		return fmt.Errorf("marshaling balance diff: %w", err)
	}

	if err := setOnBatch(b.batch, b.keyBuffer, bigIntData); err != nil {
		return fmt.Errorf("storing balance diff for ledger %d account %s asset %s: %w", diff.LedgerID, diff.Account, diff.Asset, err)
	}

	return nil
}

// SetBalanceBase stores a balance base (compacted snapshot) for an account/asset pair.
func (b *Batch) SetBalanceBase(base BalanceBase) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, base.LedgerID)
	writeByte(b.keyBuffer, keyPrefixBalanceBase)
	writeString(b.keyBuffer, base.Account)
	writeString(b.keyBuffer, base.Asset)
	writeUInt64(b.keyBuffer, base.RaftIndex)

	bigIntData, err := b.marshalOptions.MarshalAppend(b.protoBuffer, base.Balance)
	if err != nil {
		return fmt.Errorf("marshaling balance base: %w", err)
	}

	if err := setOnBatch(b.batch, b.keyBuffer, bigIntData); err != nil {
		return fmt.Errorf("storing balance base for ledger %d account %s asset %s: %w", base.LedgerID, base.Account, base.Asset, err)
	}

	return nil
}

// AppendMetadataDiff appends a metadata diff for an account key.
// If diff.Value is nil, it represents a deletion of the key (stored as empty value).
func (b *Batch) AppendMetadataDiff(diff MetadataDiff) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, diff.LedgerID)
	writeByte(b.keyBuffer, keyPrefixMetadataDiff)
	writeString(b.keyBuffer, diff.Account)
	writeString(b.keyBuffer, diff.Key)
	writeUInt64(b.keyBuffer, diff.RaftIndex)

	var valueBytes []byte
	if diff.Value != nil {
		valueBytes = []byte(*diff.Value)
	}
	// nil Value means deletion, stored as empty value

	if err := setOnBatch(b.batch, b.keyBuffer, valueBytes); err != nil {
		return fmt.Errorf("appending metadata diff: %w", err)
	}

	return nil
}

// StoreTransactionID stores the sequence associated to a transaction ID.
func (b *Batch) StoreTransactionID(ledger uint32, transactionID uint64, sequence uint64) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, ledger)
	writeByte(b.keyBuffer, keyPrefixTransactionID)
	writeUInt64(b.keyBuffer, transactionID)

	seqValue := make([]byte, 8)
	binary.BigEndian.PutUint64(seqValue, sequence)
	if err := setOnBatch(b.batch, b.keyBuffer, seqValue); err != nil {
		return fmt.Errorf("storing transaction ID mapping: %w", err)
	}

	return nil
}

// StoreRevertedTransactionID stores the sequence associated to a transaction ID that has been reverted.
func (b *Batch) StoreRevertedTransactionID(ledger uint32, transactionID uint64, sequence uint64) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, ledger)
	writeByte(b.keyBuffer, keyPrefixRevertedTxID)
	writeUInt64(b.keyBuffer, transactionID)

	seqValue := make([]byte, 8)
	binary.BigEndian.PutUint64(seqValue, sequence)
	if err := setOnBatch(b.batch, b.keyBuffer, seqValue); err != nil {
		return fmt.Errorf("storing reverted transaction ID: %w", err)
	}

	return nil
}

// Cancel cancels the batch and releases resources.
func (b *Batch) Cancel() error {
	if b.committed {
		return nil
	}

	if b.batch != nil {
		return b.batch.Close()
	}
	return nil
}

// Commit commits all buffered operations atomically with NoSync.
func (b *Batch) Commit() error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	// Write lastAppliedIndex
	if b.lastAppliedIndex > 0 {
		writeByte(b.keyBuffer, keyPrefixLastAppliedIndex)
		lastAppliedIndexValue := make([]byte, 8)
		binary.BigEndian.PutUint64(lastAppliedIndexValue, b.lastAppliedIndex)
		if err := setOnBatch(b.batch, b.keyBuffer, lastAppliedIndexValue); err != nil {
			return fmt.Errorf("updating last applied index: %w", err)
		}
	}

	// Commit with NoSync for performance
	if err := b.batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	b.committed = true
	return nil
}
