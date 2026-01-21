package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	stdtime "time"

	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

var _ store.Batch = (*Batch)(nil)

// Batch implements store.Batch using a SQL transaction for atomic operations.
type Batch struct {
	store            *Store
	tx               *sql.Tx
	lastAppliedIndex uint64
	committed        bool
}

// NewBatch creates a new Batch for atomic operations.
func (s *Store) NewBatch(lastAppliedIndex uint64) store.Batch {
	ctx := context.Background()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		// Return a batch that will fail on any operation
		return &Batch{
			store:            s,
			tx:               nil,
			lastAppliedIndex: lastAppliedIndex,
			committed:        true, // Mark as committed to prevent operations
		}
	}

	return &Batch{
		store:            s,
		tx:               tx,
		lastAppliedIndex: lastAppliedIndex,
	}
}

// RegisterLedger registers a new ledger in the store.
func (b *Batch) RegisterLedger(ctx context.Context, info *ledgerpb.LedgerInfo) error {
	if b.committed || b.tx == nil {
		return fmt.Errorf("batch already committed or invalid")
	}

	stmt := b.tx.StmtContext(ctx, b.store.stmtInsertLedger)
	defer func() { _ = stmt.Close() }()

	var metadataJSON sql.NullString
	if len(info.Metadata) > 0 {
		metadataBytes, err := json.Marshal(info.Metadata)
		if err != nil {
			return fmt.Errorf("marshaling ledger metadata: %w", err)
		}
		metadataJSON = sql.NullString{String: string(metadataBytes), Valid: true}
	}

	var createdAtStr sql.NullString
	if info.CreatedAt != nil {
		createdAtStr = sql.NullString{
			String: info.CreatedAt.AsTime().Format(stdtime.RFC3339),
			Valid:  true,
		}
	}

	if _, err := stmt.ExecContext(ctx, info.Id, info.Name, metadataJSON, createdAtStr); err != nil {
		return fmt.Errorf("inserting ledger: %w", err)
	}

	return nil
}

// AppendLogs appends logs to the batch.
func (b *Batch) AppendLogs(ctx context.Context, logs ...*ledgerpb.Log) error {
	if b.committed || b.tx == nil {
		return fmt.Errorf("batch already committed or invalid")
	}

	stmt := b.tx.StmtContext(ctx, b.store.stmtInsertLog)
	defer func() { _ = stmt.Close() }()

	for _, log := range logs {
		if log.Data == nil {
			return fmt.Errorf("log data is nil for id %d", log.Id)
		}

		dataBinary, err := proto.Marshal(log.Data)
		if err != nil {
			return fmt.Errorf("marshaling log payload to protobuf: %w", err)
		}

		var dateStr string
		if log.Date != nil {
			dateStr = log.Date.AsTime().Format(stdtime.RFC3339)
		}

		var idempotencyKey sql.NullString
		var idempotencyHash sql.Null[[]byte]
		if log.Idempotency != nil && log.Idempotency.Key != "" {
			idempotencyKey = sql.NullString{String: log.Idempotency.Key, Valid: true}
			idempotencyHash = sql.Null[[]byte]{V: log.Idempotency.Hash, Valid: true}
		}

		var id sql.NullInt64
		if log.Id != 0 {
			id = sql.NullInt64{Int64: int64(log.Id), Valid: true}
		}

		_, err = stmt.ExecContext(ctx, log.LedgerId, dataBinary,
			sql.NullString{String: dateStr, Valid: dateStr != ""},
			idempotencyKey, idempotencyHash, id)
		if err != nil {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
				return fmt.Errorf("inserting log: %w", err)
			}
		}
	}

	return nil
}

// AppendBalanceDiff appends a balance diff for an account/asset pair.
func (b *Batch) AppendBalanceDiff(ctx context.Context, ledger uint32, account, asset string, diff *ledgerpb.BigInt, logID uint64) error {
	if b.committed || b.tx == nil {
		return fmt.Errorf("batch already committed or invalid")
	}

	stmt := b.tx.StmtContext(ctx, b.store.stmtInsertBalance)
	defer func() { _ = stmt.Close() }()

	if _, err := stmt.ExecContext(ctx, ledger, account, asset, diff.Value().String()); err != nil {
		return fmt.Errorf("updating balance: %w", err)
	}

	return nil
}

// SaveAccountMetadata saves metadata for an account.
func (b *Batch) SaveAccountMetadata(ctx context.Context, ledger uint32, account string, metadata *ledgerpb.Metadata) error {
	if b.committed || b.tx == nil {
		return fmt.Errorf("batch already committed or invalid")
	}

	if metadata == nil {
		return nil
	}

	stmt := b.tx.StmtContext(ctx, b.store.stmtUpsertAccountMetadata)
	defer func() { _ = stmt.Close() }()

	for metaKey, value := range metadata.Entries {
		valueJSON, err := json.Marshal(value)
		if err != nil {
			return fmt.Errorf("marshaling metadata value: %w", err)
		}
		if _, err := stmt.ExecContext(ctx, ledger, account, metaKey, string(valueJSON)); err != nil {
			return fmt.Errorf("upserting account metadata: %w", err)
		}
	}

	return nil
}

// DeleteAccountMetadata deletes metadata keys for an account.
func (b *Batch) DeleteAccountMetadata(ctx context.Context, ledger uint32, account string, keys []string) error {
	if b.committed || b.tx == nil {
		return fmt.Errorf("batch already committed or invalid")
	}

	stmt := b.tx.StmtContext(ctx, b.store.stmtDeleteAccountMetadata)
	defer func() { _ = stmt.Close() }()

	for _, metaKey := range keys {
		if _, err := stmt.ExecContext(ctx, ledger, account, metaKey); err != nil {
			return fmt.Errorf("deleting account metadata key: %w", err)
		}
	}

	return nil
}

// StoreTransactionID stores the log ID associated to a transaction ID.
func (b *Batch) StoreTransactionID(ctx context.Context, ledger uint32, transactionID uint64, logID uint64) error {
	if b.committed || b.tx == nil {
		return fmt.Errorf("batch already committed or invalid")
	}

	stmt := b.tx.StmtContext(ctx, b.store.stmtInsertTransactionID)
	defer func() { _ = stmt.Close() }()

	if _, err := stmt.ExecContext(ctx, ledger, transactionID, logID); err != nil {
		return fmt.Errorf("storing transaction ID mapping: %w", err)
	}

	return nil
}

// StoreRevertedTransactionID stores the log ID associated to a transaction ID that has been reverted.
func (b *Batch) StoreRevertedTransactionID(ctx context.Context, ledger uint32, transactionID uint64, logID uint64) error {
	if b.committed || b.tx == nil {
		return fmt.Errorf("batch already committed or invalid")
	}

	stmt := b.tx.StmtContext(ctx, b.store.stmtInsertRevertedTransactionID)
	defer func() { _ = stmt.Close() }()

	if _, err := stmt.ExecContext(ctx, ledger, transactionID, logID); err != nil {
		return fmt.Errorf("storing reverted transaction ID: %w", err)
	}

	return nil
}

// DeleteLedger deletes all data for a ledger by its ID.
func (b *Batch) DeleteLedger(ctx context.Context, id uint32) error {
	if b.committed || b.tx == nil {
		return fmt.Errorf("batch already committed or invalid")
	}

	// Delete from all tables that have ledger data (now keyed by ledger ID)
	tables := []string{
		"logs",
		"balances",
		"account_metadata",
		"transaction_ids",
		"reverted_transaction_ids",
	}

	for _, table := range tables {
		_, err := b.tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE ledger = ?", table), id)
		if err != nil {
			return fmt.Errorf("deleting from %s: %w", table, err)
		}
	}

	// Delete the ledger entry itself
	_, err := b.tx.ExecContext(ctx, `DELETE FROM ledgers WHERE id = ?`, id)
	if err != nil {
		return fmt.Errorf("deleting ledger: %w", err)
	}

	return nil
}

// Cancel cancels the batch and releases resources.
func (b *Batch) Cancel(ctx context.Context) error {
	if b.committed {
		return nil
	}

	if b.tx != nil {
		return b.tx.Rollback()
	}
	return nil
}

// Commit commits all buffered operations atomically.
func (b *Batch) Commit(ctx context.Context) error {
	if b.committed {
		return fmt.Errorf("batch already committed")
	}

	if b.tx == nil {
		return fmt.Errorf("batch is invalid (no transaction)")
	}

	// Update the lastAppliedIndex
	if b.lastAppliedIndex > 0 {
		_, err := b.tx.ExecContext(ctx, `INSERT OR REPLACE INTO raft_state (key, value) VALUES ('last_applied_index', ?)`, b.lastAppliedIndex)
		if err != nil {
			return fmt.Errorf("updating last applied index: %w", err)
		}
	}

	if err := b.tx.Commit(); err != nil {
		return fmt.Errorf("committing transaction: %w", err)
	}

	b.committed = true
	return nil
}
