package store

import (
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
	KeyBuilder     *KeyBuilder
	protoBuffer    []byte
	committed      bool
	marshalOptions proto.MarshalOptions
}

// NewBatch creates a new Batch for atomic operations.
func (s *Store) NewBatch() *Batch {
	return &Batch{
		store:       s,
		batch:       s.db.NewBatch(),
		KeyBuilder:  NewKeyBuilder(),
		protoBuffer: make([]byte, 0, 1024),
	}
}

// SetAppliedIndex writes the last applied Raft index to the batch.
func (b *Batch) SetAppliedIndex(index uint64) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	b.KeyBuilder.PutByte(keyPrefixLastAppliedIndex)
	lastAppliedIndexValue := make([]byte, 8)
	binary.BigEndian.PutUint64(lastAppliedIndexValue, index)
	if err := b.batch.Set(b.KeyBuilder.Build(), lastAppliedIndexValue, pebble.NoSync); err != nil {
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

		b.KeyBuilder.
			PutByte(keyPrefixLog).
			PutUInt64(log.Sequence)

		if err := b.batch.Set(b.KeyBuilder.Build(), logBinary, pebble.NoSync); err != nil {
			return fmt.Errorf("inserting system log: %w", err)
		}

		// Create idempotency index if present
		if log.Idempotency != nil && log.Idempotency.Key != "" {
			seqValue := make([]byte, 8)
			binary.BigEndian.PutUint64(seqValue, log.Sequence)

			b.KeyBuilder.
				PutByte(keyPrefixIdempotency).
				PutString(log.Idempotency.Key)
			if err := b.batch.Set(b.KeyBuilder.Build(), seqValue, pebble.NoSync); err != nil {
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
	b.KeyBuilder.
		PutByte(keyPrefixLedgerInfo).
		PutUInt32(info.Id)

	if err := b.batch.Set(b.KeyBuilder.Build(), infoBinary, pebble.NoSync); err != nil {
		return fmt.Errorf("inserting ledger info: %w", err)
	}

	return nil
}

// AppendBalanceDiff appends a balance diff for an account/asset pair.
func (b *Batch) AppendBalanceDiff(key TimestampedBalanceKey, diff *commonpb.BigInt) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	b.KeyBuilder.
		PutLedgerPrefix(key.LedgerName).
		PutByte(keyPrefixBalanceDiff).
		PutString(key.Account).
		PutString(key.Asset).
		PutUInt64(key.RaftIndex)

	bigIntData, err := b.marshalOptions.MarshalAppend(b.protoBuffer, diff)
	if err != nil {
		return fmt.Errorf("marshaling balance diff: %w", err)
	}

	if err := b.batch.Set(b.KeyBuilder.Build(), bigIntData, pebble.NoSync); err != nil {
		return fmt.Errorf("storing balance diff for ledger %s account %s asset %s: %w", key.LedgerName, key.Account, key.Asset, err)
	}

	return nil
}

// SetBalanceBase stores a balance base (compacted snapshot) for an account/asset pair.
func (b *Batch) SetBalanceBase(key TimestampedBalanceKey, balance *commonpb.BigInt) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	b.KeyBuilder.
		PutLedgerPrefix(key.LedgerName).
		PutByte(keyPrefixBalanceBase).
		PutString(key.Account).
		PutString(key.Asset).
		PutUInt64(key.RaftIndex)

	bigIntData, err := b.marshalOptions.MarshalAppend(b.protoBuffer, balance)
	if err != nil {
		return fmt.Errorf("marshaling balance base: %w", err)
	}

	if err := b.batch.Set(b.KeyBuilder.Build(), bigIntData, pebble.NoSync); err != nil {
		return fmt.Errorf("storing balance base for ledger %s account %s asset %s: %w", key.LedgerName, key.Account, key.Asset, err)
	}

	return nil
}

// AppendMetadataDiff appends a metadata diff for an account key.
// If value is nil, it represents a deletion of the key (stored as empty value).
func (b *Batch) AppendMetadataDiff(key TimestampedMetadataKey, value *commonpb.MetadataValue) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	b.KeyBuilder.
		PutLedgerPrefix(key.LedgerName).
		PutByte(keyPrefixMetadataDiff).
		PutString(key.Account).
		PutString(key.Key).
		PutUInt64(key.RaftIndex)

	var valueBytes []byte
	if value != nil {
		var err error
		valueBytes, err = b.marshalOptions.MarshalAppend(b.protoBuffer, value)
		if err != nil {
			return fmt.Errorf("marshaling metadata diff value: %w", err)
		}
	}
	// nil value means deletion, stored as empty value

	if err := b.batch.Set(b.KeyBuilder.Build(), valueBytes, pebble.NoSync); err != nil {
		return fmt.Errorf("appending metadata diff: %w", err)
	}

	return nil
}

// SetMetadataBase stores a metadata base (compacted snapshot) for an account/key pair.
// If value is nil, it represents a deletion of the key at this base index.
func (b *Batch) SetMetadataBase(key TimestampedMetadataKey, value *commonpb.MetadataValue) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	b.KeyBuilder.
		PutLedgerPrefix(key.LedgerName).
		PutByte(keyPrefixMetadataBase).
		PutString(key.Account).
		PutString(key.Key).
		PutUInt64(key.RaftIndex)

	var valueBytes []byte
	if value != nil {
		var err error
		valueBytes, err = b.marshalOptions.MarshalAppend(b.protoBuffer, value)
		if err != nil {
			return fmt.Errorf("marshaling metadata base value: %w", err)
		}
	}
	// nil value means deletion, stored as empty value

	if err := b.batch.Set(b.KeyBuilder.Build(), valueBytes, pebble.NoSync); err != nil {
		return fmt.Errorf("storing metadata base for ledger %s account %s key %s: %w", key.LedgerName, key.Account, key.Key, err)
	}

	return nil
}

// StoreTransactionUpdate stores a transaction update (init, revert, add/delete metadata).
// Key: [ledger][keyPrefixTransactionUpdate][transactionID][byLog] -> TransactionUpdate
func (b *Batch) StoreTransactionUpdate(ledgerName string, transactionID uint64, update *commonpb.TransactionUpdate) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	b.KeyBuilder.
		PutLedgerPrefix(ledgerName).
		PutByte(keyPrefixTransactionUpdate).
		PutUInt64(transactionID).
		PutUInt64(update.ByLog)

	updateData, err := b.marshalOptions.MarshalAppend(b.protoBuffer, update)
	if err != nil {
		return fmt.Errorf("marshaling transaction update: %w", err)
	}

	if err := b.batch.Set(b.KeyBuilder.Build(), updateData, pebble.NoSync); err != nil {
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
