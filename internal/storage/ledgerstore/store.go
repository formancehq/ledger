package ledgerstore

import (
	"context"
	"fmt"

	"github.com/formancehq/go-libs/migrations"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
)

type Store struct {
	bucket *Bucket

	name string
}

func (store *Store) Name() string {
	return store.name
}

func (store *Store) GetDB() *bun.DB {
	return store.bucket.db
}

func (store *Store) IsUpToDate(ctx context.Context) (bool, error) {
	return store.bucket.IsUpToDate(ctx)
}

func (store *Store) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return store.bucket.GetMigrationsInfo(ctx)
}

// applyLedgerFilter conditionally applies the WHERE ledger = ? clause to a query.
// If the bucket contains only one ledger, the filter is skipped for performance optimization.
// The tableName parameter should include the table alias if present (e.g., "transactions" or "accounts").
func (store *Store) applyLedgerFilter(query *bun.SelectQuery, tableName string) *bun.SelectQuery {
	if store.bucket.IsSingleLedger() {
		// Skip the WHERE clause for single-ledger buckets
		return query
	}
	// Apply standard ledger filter for multi-ledger buckets
	return query.Where(tableName+".ledger = ?", store.name)
}

// getLedgerFilterSQL returns the SQL fragment and arguments for ledger filtering in inline SQL.
// Returns empty string and nil args if single-ledger optimization is enabled.
func (store *Store) getLedgerFilterSQL(tableName string) (string, []any) {
	if store.bucket.IsSingleLedger() {
		return "", nil
	}
	return fmt.Sprintf("and %s.ledger = ?", tableName), []any{store.name}
}

// getLedgerFilterSQLWithoutPrefix returns the SQL fragment for ledger filtering without table prefix.
// Used for inline subqueries where table name is already specified.
func (store *Store) getLedgerFilterSQLWithoutPrefix() (string, []any) {
	if store.bucket.IsSingleLedger() {
		return "", nil
	}
	return "and ledger = ?", []any{store.name}
}

func New(
	bucket *Bucket,
	name string,
) (*Store, error) {
	return &Store{
		bucket: bucket,
		name:   name,
	}, nil
}
