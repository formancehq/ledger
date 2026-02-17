package data

import (
	"encoding/binary"
	"fmt"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
)

// vtSizedMarshaler is implemented by vtprotobuf-generated messages.
type vtSizedMarshaler interface {
	SizeVT() int
	MarshalToVT([]byte) (int, error)
}

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

// marshalProto marshals a proto message using vtprotobuf when available,
// falling back to standard MarshalAppend otherwise.
func (b *Batch) marshalProto(msg proto.Message) ([]byte, error) {
	if m, ok := msg.(vtSizedMarshaler); ok {
		size := m.SizeVT()
		if cap(b.protoBuffer) >= size {
			b.protoBuffer = b.protoBuffer[:size]
		} else {
			b.protoBuffer = make([]byte, size)
		}
		n, err := m.MarshalToVT(b.protoBuffer)
		return b.protoBuffer[:n], err
	}
	return b.marshalOptions.MarshalAppend(b.protoBuffer, msg)
}

// NewBatch creates a new Batch for atomic operations.
func (s *Store) NewBatch() *Batch {
	return &Batch{
		store:       s,
		batch:       s.getDB().NewBatch(),
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

// SetLastAppliedTimestamp writes the last applied HLC timestamp to the batch.
func (b *Batch) SetLastAppliedTimestamp(ts uint64) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	b.KeyBuilder.PutByte(keyPrefixLastAppliedTimestamp)
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, ts)
	if err := b.batch.Set(b.KeyBuilder.Build(), value, pebble.NoSync); err != nil {
		return fmt.Errorf("updating last applied timestamp: %w", err)
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
		logBinary, err := b.marshalProto(log)
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
	infoBinary, err := b.marshalProto(info)
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

// StoreTransactionUpdate stores a transaction update (init, revert, add/delete metadata).
// Key: [ledger][keyPrefixTransactionUpdate][transactionID][byLog] -> TransactionUpdate
func (b *Batch) StoreTransactionUpdate(key TransactionKey, update *commonpb.TransactionUpdate) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	b.KeyBuilder.
		PutLedgerPrefix(key.LedgerID).
		PutByte(keyPrefixTransactionUpdate).
		PutUInt64(key.ID).
		PutUInt64(update.ByLog)

	updateData, err := b.marshalProto(update)
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

func (b *Batch) Set(key, value []byte, options *pebble.WriteOptions) error {
	return b.batch.Set(key, value, options)
}

// AppendAuditEntries appends audit entries to the batch.
func (b *Batch) AppendAuditEntries(entries ...*auditpb.AuditEntry) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	for _, entry := range entries {
		entryBinary, err := b.marshalProto(entry)
		if err != nil {
			return fmt.Errorf("marshaling audit entry to protobuf: %w", err)
		}

		b.KeyBuilder.
			PutByte(keyPrefixAudit).
			PutUInt64(entry.Sequence)

		if err := b.batch.Set(b.KeyBuilder.Build(), entryBinary, pebble.NoSync); err != nil {
			return fmt.Errorf("inserting audit entry: %w", err)
		}
	}

	return nil
}

// DeleteRange deletes all keys in the range [start, end).
func (b *Batch) DeleteRange(start, end []byte, options *pebble.WriteOptions) error {
	return b.batch.DeleteRange(start, end, options)
}
