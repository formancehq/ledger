package v2

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

// PostgresSource is a Source that reads logs directly from a v2 ledger's
// PostgreSQL database. In v2, the _system.ledgers table maps each ledger
// to a bucket (PostgreSQL schema). The logs table lives in that schema
// and uses a ledger column to distinguish between ledgers sharing a bucket.
type PostgresSource struct {
	pool       *pgxpool.Pool
	ledgerName string
	bucket     string // PostgreSQL schema containing the logs table
}

// NewPostgresSource creates a new PostgreSQL-based v2 log source.
// It looks up the bucket (schema) for the given ledger from _system.ledgers.
func NewPostgresSource(ctx context.Context, dsn, ledgerName string) (*PostgresSource, error) {
	pool, err := pgxpool.New(ctx, encodeDSNPassword(dsn))
	if err != nil {
		return nil, fmt.Errorf("creating pgx pool: %w", err)
	}

	// Look up the bucket (schema) for this ledger from _system.ledgers.
	var bucket string
	err = pool.QueryRow(ctx,
		`SELECT bucket FROM _system.ledgers WHERE name = $1`,
		ledgerName,
	).Scan(&bucket)
	if err != nil {
		pool.Close()
		return nil, fmt.Errorf("looking up bucket for ledger %q: %w", ledgerName, err)
	}

	return &PostgresSource{
		pool:       pool,
		ledgerName: ledgerName,
		bucket:     bucket,
	}, nil
}

// FetchLogs reads logs from the v2 PostgreSQL log table.
// afterID is the last known log ID (0 to start from the beginning).
// Returns logs (oldest first), whether there are more, and any error.
func (s *PostgresSource) FetchLogs(ctx context.Context, afterID uint64, pageSize int) ([]V2Log, bool, error) {
	// Fetch pageSize+1 rows to determine if there are more.
	query := fmt.Sprintf(
		`SELECT id, type, date::text, data, encode(hash, 'hex') FROM %q.logs WHERE ledger = $1 AND id > $2 ORDER BY id ASC LIMIT $3`,
		s.bucket,
	)

	rows, err := s.pool.Query(ctx, query, s.ledgerName, afterID, pageSize+1)
	if err != nil {
		return nil, false, fmt.Errorf("querying logs: %w", err)
	}
	defer rows.Close()

	var logs []V2Log
	for rows.Next() {
		var l V2Log
		var data []byte
		if err := rows.Scan(&l.ID, &l.Type, &l.Date, &data, &l.Hash); err != nil {
			return nil, false, fmt.Errorf("scanning log row: %w", err)
		}
		l.Data = json.RawMessage(data)
		logs = append(logs, l)
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("iterating log rows: %w", err)
	}

	hasMore := len(logs) > pageSize
	if hasMore {
		logs = logs[:pageSize]
	}

	return logs, hasMore, nil
}

// GetLatestLogID returns the highest log ID from the v2 PostgreSQL log table.
// Returns 0 if the table is empty.
func (s *PostgresSource) GetLatestLogID(ctx context.Context) (uint64, error) {
	query := fmt.Sprintf(
		`SELECT COALESCE(MAX(id), 0) FROM %q.logs WHERE ledger = $1`,
		s.bucket,
	)

	var maxID uint64
	if err := s.pool.QueryRow(ctx, query, s.ledgerName).Scan(&maxID); err != nil {
		return 0, fmt.Errorf("querying latest log ID: %w", err)
	}

	return maxID, nil
}

// Close closes the underlying connection pool.
func (s *PostgresSource) Close() error {
	s.pool.Close()
	return nil
}

// encodeDSNPassword ensures that passwords containing URL-special characters
// (e.g. |, ?, #, [, ]) are properly percent-encoded so pgx can parse the DSN.
// Only modifies URL-format DSNs (postgres:// or postgresql://).
func encodeDSNPassword(dsn string) string {
	schemeEnd := strings.Index(dsn, "://")
	if schemeEnd == -1 {
		return dsn
	}

	rest := dsn[schemeEnd+3:]

	lastAt := strings.LastIndex(rest, "@")
	if lastAt == -1 {
		return dsn
	}

	creds := rest[:lastAt]
	hostPart := rest[lastAt:]

	colonIdx := strings.Index(creds, ":")
	if colonIdx == -1 {
		return dsn
	}

	password := creds[colonIdx+1:]
	encoded := url.PathEscape(password)
	encoded = strings.ReplaceAll(encoded, "@", "%40")
	if encoded == password {
		return dsn
	}

	return dsn[:schemeEnd+3] + creds[:colonIdx+1] + encoded + hostPart
}
