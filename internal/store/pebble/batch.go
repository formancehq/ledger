package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

var _ store.Batch = (*Batch)(nil)

// Batch implements store.Batch using a pebble.Batch with NoSync for atomic operations.
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
func (s *Store) NewBatch(lastAppliedIndex uint64) store.Batch {
	return &Batch{
		store:            s,
		batch:            s.db.NewBatch(),
		lastAppliedIndex: lastAppliedIndex,
		keyBuffer:        bytes.NewBuffer(make([]byte, 0, 1024)),
		protoBuffer:      make([]byte, 0, 1024),
	}
}

// RegisterLedger registers a new ledger in the store.
func (b *Batch) RegisterLedger(ctx context.Context, info *commonpb.LedgerInfo) error {
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

// AppendLogs appends logs to the batch.
func (b *Batch) AppendLogs(ctx context.Context, logs ...*commonpb.Log) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	for _, log := range logs {
		// Marshal protobuf Log to binary
		logBinary, err := b.marshalOptions.MarshalAppend(b.protoBuffer, log)
		if err != nil {
			return fmt.Errorf("marshaling log to protobuf: %w", err)
		}

		writeLedgerPrefix(b.keyBuffer, log.LedgerId)
		writeByte(b.keyBuffer, keyPrefixLog)
		writeUInt64(b.keyBuffer, log.Id)

		if err := setOnBatch(b.batch, b.keyBuffer, logBinary); err != nil {
			return fmt.Errorf("inserting log: %w", err)
		}

		// Also create an index by idempotency key if present
		if log.Idempotency != nil && log.Idempotency.Key != "" {
			writeLedgerPrefix(b.keyBuffer, log.LedgerId)
			writeByte(b.keyBuffer, keyPrefixIdempotency)
			writeString(b.keyBuffer, log.Idempotency.Key)
			// Store the log ID as value for quick lookup
			idValue := make([]byte, 8)
			binary.BigEndian.PutUint64(idValue, log.Id)
			if err := setOnBatch(b.batch, b.keyBuffer, idValue); err != nil {
				return fmt.Errorf("inserting idempotency index: %w", err)
			}
		}
	}

	return nil
}

// AppendBalanceDiff appends a balance diff for an account/asset pair.
func (b *Batch) AppendBalanceDiff(ctx context.Context, ledger uint32, account, asset string, diff *commonpb.BigInt, logID uint64) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, ledger)
	writeByte(b.keyBuffer, keyPrefixBalanceDiff)
	writeString(b.keyBuffer, account)
	writeString(b.keyBuffer, asset)
	writeUInt64(b.keyBuffer, logID)

	bigIntData, err := b.marshalOptions.MarshalAppend(b.protoBuffer, diff)
	if err != nil {
		return fmt.Errorf("marshaling balance diff: %w", err)
	}

	if err := setOnBatch(b.batch, b.keyBuffer, bigIntData); err != nil {
		return fmt.Errorf("storing balance diff for ledger %d account %s asset %s: %w", ledger, account, asset, err)
	}

	return nil
}

// SaveAccountMetadata saves metadata for an account.
func (b *Batch) SaveAccountMetadata(ctx context.Context, ledger uint32, account string, metadata *commonpb.Metadata) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	if metadata == nil {
		return nil
	}

	for metaKey, value := range metadata.Entries {
		writeLedgerPrefix(b.keyBuffer, ledger)
		writeByte(b.keyBuffer, keyPrefixAccountMetadata)
		writeString(b.keyBuffer, account)
		writeString(b.keyBuffer, metaKey)

		if err := setOnBatch(b.batch, b.keyBuffer, []byte(value)); err != nil {
			return fmt.Errorf("upserting account metadata: %w", err)
		}
	}

	return nil
}

// DeleteAccountMetadata deletes metadata keys for an account.
func (b *Batch) DeleteAccountMetadata(ctx context.Context, ledger uint32, account string, keys []string) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	for _, metaKey := range keys {
		writeLedgerPrefix(b.keyBuffer, ledger)
		writeByte(b.keyBuffer, keyPrefixAccountMetadata)
		writeString(b.keyBuffer, account)
		writeString(b.keyBuffer, metaKey)

		if err := deleteOnBatch(b.batch, b.keyBuffer); err != nil {
			return fmt.Errorf("deleting account metadata key: %w", err)
		}
	}

	return nil
}

// StoreTransactionID stores the log ID associated to a transaction ID.
func (b *Batch) StoreTransactionID(ctx context.Context, ledger uint32, transactionID uint64, logID uint64) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, ledger)
	writeByte(b.keyBuffer, keyPrefixTransactionID)
	writeUInt64(b.keyBuffer, transactionID)

	logIDValue := make([]byte, 8)
	binary.BigEndian.PutUint64(logIDValue, logID)
	if err := setOnBatch(b.batch, b.keyBuffer, logIDValue); err != nil {
		return fmt.Errorf("storing transaction ID mapping: %w", err)
	}

	return nil
}

// StoreRevertedTransactionID stores the log ID associated to a transaction ID that has been reverted.
func (b *Batch) StoreRevertedTransactionID(ctx context.Context, ledger uint32, transactionID uint64, logID uint64) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.keyBuffer, ledger)
	writeByte(b.keyBuffer, keyPrefixRevertedTxID)
	writeUInt64(b.keyBuffer, transactionID)

	logIDValue := make([]byte, 8)
	binary.BigEndian.PutUint64(logIDValue, logID)
	if err := setOnBatch(b.batch, b.keyBuffer, logIDValue); err != nil {
		return fmt.Errorf("storing reverted transaction ID: %w", err)
	}

	return nil
}

// DeleteLedger deletes all data for a ledger by its ID.
func (b *Batch) DeleteLedger(ctx context.Context, id uint32) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	// First, get the ledger info to find the name
	writeByte(b.keyBuffer, keyPrefixLedgerInfo)
	if err := binary.Write(b.keyBuffer, binary.BigEndian, id); err != nil {
		return fmt.Errorf("writing ledger ID: %w", err)
	}
	ledgerInfoKey := make([]byte, b.keyBuffer.Len())
	copy(ledgerInfoKey, b.keyBuffer.Bytes())
	b.keyBuffer.Reset()

	value, closer, err := b.store.db.Get(ledgerInfoKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil // Ledger doesn't exist, nothing to delete
		}
		return fmt.Errorf("getting ledger info: %w", err)
	}

	info := &commonpb.LedgerInfo{}
	if err := proto.Unmarshal(value, info); err != nil {
		_ = closer.Close()
		return fmt.Errorf("unmarshaling ledger info: %w", err)
	}
	_ = closer.Close()

	// Delete all data for this ledger ID
	startBuf := bytes.NewBuffer(nil)
	writeLedgerPrefix(startBuf, info.Id)

	endBuf := bytes.NewBuffer(nil)
	writeLedgerPrefix(endBuf, info.Id)
	writeByte(endBuf, 0xFF)

	if err := b.batch.DeleteRange(startBuf.Bytes(), endBuf.Bytes(), pebble.NoSync); err != nil {
		return fmt.Errorf("deleting ledger range: %w", err)
	}

	// Delete the ledger info entry
	if err := b.batch.Delete(ledgerInfoKey, pebble.NoSync); err != nil {
		return fmt.Errorf("deleting ledger info: %w", err)
	}

	return nil
}

// Cancel cancels the batch and releases resources.
func (b *Batch) Cancel(ctx context.Context) error {
	if b.committed {
		return nil
	}

	if b.batch != nil {
		return b.batch.Close()
	}
	return nil
}

// Commit commits all buffered operations atomically with NoSync.
func (b *Batch) Commit(ctx context.Context) error {
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
