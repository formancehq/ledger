package pebble

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"time"

	"github.com/cockroachdb/pebble"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

var _ store.Batch = (*Batch)(nil)

// Batch implements store.Batch using a pebble.Batch with NoSync for atomic operations.
type Batch struct {
	store            *Store
	batch            *pebble.Batch
	lastAppliedIndex uint64
	buf              *bytes.Buffer
	committed        bool
}

// NewBatch creates a new Batch for atomic operations.
func (s *Store) NewBatch(lastAppliedIndex uint64) store.Batch {
	return &Batch{
		store:            s,
		batch:            s.db.NewBatch(),
		lastAppliedIndex: lastAppliedIndex,
		buf:              bytes.NewBuffer(make([]byte, 0, 1024)),
	}
}

// RegisterLedger registers a new ledger in the store.
func (b *Batch) RegisterLedger(ctx context.Context, info *ledgerpb.LedgerInfo) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	// Marshal LedgerInfo to protobuf
	infoBinary, err := proto.Marshal(info)
	if err != nil {
		return fmt.Errorf("marshaling ledger info to protobuf: %w", err)
	}

	// Store with key: prefix + ledger ID (4 bytes big-endian)
	writeByte(b.buf, keyPrefixLedgerInfo)
	if err := binary.Write(b.buf, binary.BigEndian, info.Id); err != nil {
		return fmt.Errorf("writing ledger ID: %w", err)
	}

	if err := setOnBatch(b.batch, b.buf, infoBinary); err != nil {
		return fmt.Errorf("inserting ledger info: %w", err)
	}

	return nil
}

// AppendLogs appends logs to the batch.
func (b *Batch) AppendLogs(ctx context.Context, logs ...*ledgerpb.Log) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	for _, log := range logs {
		// Marshal protobuf Log to binary
		logBinary, err := proto.Marshal(log)
		if err != nil {
			return fmt.Errorf("marshaling log to protobuf: %w", err)
		}

		writeLedgerPrefix(b.buf, log.LedgerId)
		writeByte(b.buf, keyPrefixLog)
		writeUInt64(b.buf, log.Id)

		if err := setOnBatch(b.batch, b.buf, logBinary); err != nil {
			return fmt.Errorf("inserting log: %w", err)
		}

		// Also create an index by idempotency key if present
		if log.Idempotency != nil && log.Idempotency.Key != "" {
			writeLedgerPrefix(b.buf, log.LedgerId)
			writeByte(b.buf, keyPrefixIdempotency)
			writeString(b.buf, log.Idempotency.Key)
			// Store the log ID as value for quick lookup
			idValue := make([]byte, 8)
			binary.BigEndian.PutUint64(idValue, log.Id)
			if err := setOnBatch(b.batch, b.buf, idValue); err != nil {
				return fmt.Errorf("inserting idempotency index: %w", err)
			}
		}
	}

	return nil
}

// AppendBalanceDiff appends a balance diff for an account/asset pair.
func (b *Batch) AppendBalanceDiff(ctx context.Context, ledger uint32, account, asset string, diff *ledgerpb.BigInt) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.buf, ledger)
	writeByte(b.buf, keyPrefixBalanceDiff)
	writeString(b.buf, account)
	writeString(b.buf, asset)
	writeInt64(b.buf, time.Now().UnixNano())

	bigIntData, err := proto.Marshal(diff)
	if err != nil {
		return fmt.Errorf("marshaling balance diff: %w", err)
	}

	if err := setOnBatch(b.batch, b.buf, bigIntData); err != nil {
		return fmt.Errorf("storing balance diff for ledger %d account %s asset %s: %w", ledger, account, asset, err)
	}

	return nil
}

// SaveAccountMetadata saves metadata for an account.
func (b *Batch) SaveAccountMetadata(ctx context.Context, ledger uint32, account string, metadata *ledgerpb.Metadata) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	if metadata == nil {
		return nil
	}

	for metaKey, value := range metadata.Entries {
		writeLedgerPrefix(b.buf, ledger)
		writeByte(b.buf, keyPrefixAccountMetadata)
		writeString(b.buf, account)
		writeString(b.buf, metaKey)

		if err := setOnBatch(b.batch, b.buf, []byte(value)); err != nil {
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
		writeLedgerPrefix(b.buf, ledger)
		writeByte(b.buf, keyPrefixAccountMetadata)
		writeString(b.buf, account)
		writeString(b.buf, metaKey)

		if err := deleteOnBatch(b.batch, b.buf); err != nil {
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

	writeLedgerPrefix(b.buf, ledger)
	writeByte(b.buf, keyPrefixTransactionID)
	writeUInt64(b.buf, transactionID)

	logIDValue := make([]byte, 8)
	binary.BigEndian.PutUint64(logIDValue, logID)
	if err := setOnBatch(b.batch, b.buf, logIDValue); err != nil {
		return fmt.Errorf("storing transaction ID mapping: %w", err)
	}

	return nil
}

// StoreRevertedTransactionID stores the log ID associated to a transaction ID that has been reverted.
func (b *Batch) StoreRevertedTransactionID(ctx context.Context, ledger uint32, transactionID uint64, logID uint64) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	writeLedgerPrefix(b.buf, ledger)
	writeByte(b.buf, keyPrefixRevertedTxID)
	writeUInt64(b.buf, transactionID)

	logIDValue := make([]byte, 8)
	binary.BigEndian.PutUint64(logIDValue, logID)
	if err := setOnBatch(b.batch, b.buf, logIDValue); err != nil {
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
	writeByte(b.buf, keyPrefixLedgerInfo)
	if err := binary.Write(b.buf, binary.BigEndian, id); err != nil {
		return fmt.Errorf("writing ledger ID: %w", err)
	}
	ledgerInfoKey := make([]byte, b.buf.Len())
	copy(ledgerInfoKey, b.buf.Bytes())
	b.buf.Reset()

	value, closer, err := b.store.db.Get(ledgerInfoKey)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil // Ledger doesn't exist, nothing to delete
		}
		return fmt.Errorf("getting ledger info: %w", err)
	}

	info := &ledgerpb.LedgerInfo{}
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
		writeByte(b.buf, keyPrefixLastAppliedIndex)
		lastAppliedIndexValue := make([]byte, 8)
		binary.BigEndian.PutUint64(lastAppliedIndexValue, b.lastAppliedIndex)
		if err := setOnBatch(b.batch, b.buf, lastAppliedIndexValue); err != nil {
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
