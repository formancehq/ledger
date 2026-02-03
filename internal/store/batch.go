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
	store          *Store
	batch          *pebble.Batch
	keyBuffer      *bytes.Buffer
	protoBuffer    []byte
	committed      bool
	marshalOptions proto.MarshalOptions
}

// NewBatch creates a new Batch for atomic operations.
func (s *Store) NewBatch() *Batch {
	return &Batch{
		store:       s,
		batch:       s.db.NewBatch(),
		keyBuffer:   bytes.NewBuffer(make([]byte, 0, 1024)),
		protoBuffer: make([]byte, 0, 1024),
	}
}

// SetAppliedIndex writes the last applied Raft index to the batch.
func (b *Batch) SetAppliedIndex(index uint64) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeByte(b.keyBuffer, keyPrefixLastAppliedIndex)
	lastAppliedIndexValue := make([]byte, 8)
	binary.BigEndian.PutUint64(lastAppliedIndexValue, index)
	if err := setOnBatch(b.batch, b.keyBuffer, lastAppliedIndexValue); err != nil {
		return fmt.Errorf("updating last applied index: %w", err)
	}
	return nil
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
func (b *Batch) AppendBalanceDiff(key BalanceKey, diff BalanceDiff) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, key.LedgerID)
	writeByte(b.keyBuffer, keyPrefixBalanceDiff)
	writeString(b.keyBuffer, key.Account)
	writeString(b.keyBuffer, key.Asset)
	writeUInt64(b.keyBuffer, key.RaftIndex)

	bigIntData, err := b.marshalOptions.MarshalAppend(b.protoBuffer, diff.Diff)
	if err != nil {
		return fmt.Errorf("marshaling balance diff: %w", err)
	}

	if err := setOnBatch(b.batch, b.keyBuffer, bigIntData); err != nil {
		return fmt.Errorf("storing balance diff for ledger %d account %s asset %s: %w", key.LedgerID, key.Account, key.Asset, err)
	}

	return nil
}

// SetBalanceBase stores a balance base (compacted snapshot) for an account/asset pair.
func (b *Batch) SetBalanceBase(key BalanceKey, base BalanceBase) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, key.LedgerID)
	writeByte(b.keyBuffer, keyPrefixBalanceBase)
	writeString(b.keyBuffer, key.Account)
	writeString(b.keyBuffer, key.Asset)
	writeUInt64(b.keyBuffer, key.RaftIndex)

	bigIntData, err := b.marshalOptions.MarshalAppend(b.protoBuffer, base.Balance)
	if err != nil {
		return fmt.Errorf("marshaling balance base: %w", err)
	}

	if err := setOnBatch(b.batch, b.keyBuffer, bigIntData); err != nil {
		return fmt.Errorf("storing balance base for ledger %d account %s asset %s: %w", key.LedgerID, key.Account, key.Asset, err)
	}

	return nil
}

// AppendMetadataDiff appends a metadata diff for an account key.
// If diff.Value is nil, it represents a deletion of the key (stored as empty value).
func (b *Batch) AppendMetadataDiff(key MetadataKey, diff MetadataDiff) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, key.LedgerID)
	writeByte(b.keyBuffer, keyPrefixMetadataDiff)
	writeString(b.keyBuffer, key.Account)
	writeString(b.keyBuffer, key.Key)
	writeUInt64(b.keyBuffer, key.RaftIndex)

	var valueBytes []byte
	if diff.Value != nil {
		var err error
		valueBytes, err = b.marshalOptions.MarshalAppend(b.protoBuffer, diff.Value)
		if err != nil {
			return fmt.Errorf("marshaling metadata diff value: %w", err)
		}
	}
	// nil Value means deletion, stored as empty value

	if err := setOnBatch(b.batch, b.keyBuffer, valueBytes); err != nil {
		return fmt.Errorf("appending metadata diff: %w", err)
	}

	return nil
}

// SetMetadataBase stores a metadata base (compacted snapshot) for an account/key pair.
// If base.Value is nil, it represents a deletion of the key at this base index.
func (b *Batch) SetMetadataBase(key MetadataKey, base MetadataBase) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, key.LedgerID)
	writeByte(b.keyBuffer, keyPrefixMetadataBase)
	writeString(b.keyBuffer, key.Account)
	writeString(b.keyBuffer, key.Key)
	writeUInt64(b.keyBuffer, key.RaftIndex)

	var valueBytes []byte
	if base.Value != nil {
		var err error
		valueBytes, err = b.marshalOptions.MarshalAppend(b.protoBuffer, base.Value)
		if err != nil {
			return fmt.Errorf("marshaling metadata base value: %w", err)
		}
	}
	// nil Value means deletion, stored as empty value

	if err := setOnBatch(b.batch, b.keyBuffer, valueBytes); err != nil {
		return fmt.Errorf("storing metadata base for ledger %d account %s key %s: %w", key.LedgerID, key.Account, key.Key, err)
	}

	return nil
}

// StoreTransactionUpdate stores a transaction update (init, revert, add/delete metadata).
// Key: [ledger][keyPrefixTransactionUpdate][transactionID][byLog] -> TransactionUpdate
func (b *Batch) StoreTransactionUpdate(ledger uint32, transactionID uint64, update *commonpb.TransactionUpdate) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, ledger)
	writeByte(b.keyBuffer, keyPrefixTransactionUpdate)
	writeUInt64(b.keyBuffer, transactionID)
	writeUInt64(b.keyBuffer, update.ByLog)

	updateData, err := b.marshalOptions.MarshalAppend(b.protoBuffer, update)
	if err != nil {
		return fmt.Errorf("marshaling transaction update: %w", err)
	}

	if err := setOnBatch(b.batch, b.keyBuffer, updateData); err != nil {
		return fmt.Errorf("storing transaction update: %w", err)
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

	// Commit with NoSync for performance
	if err := b.batch.Commit(pebble.NoSync); err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	b.committed = true
	return nil
}
