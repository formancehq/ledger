package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strings"
	stdtime "time"

	"github.com/XSAM/otelsql"
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/metadata"
	"github.com/formancehq/go-libs/v3/time"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger-v3-poc/internal/json"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
	"github.com/formancehq/ledger-v3-poc/internal/store"
)

var _ store.Store = (*Store)(nil)

// Store is a SQLite implementation of store.Store
type Store struct {
	db     *DB
	logger logging.Logger

	// Prepared statements
	stmtGetLogByID                  *sql.Stmt
	stmtInsertLog                   *sql.Stmt
	stmtGetIdempotency              *sql.Stmt
	stmtInsertBalance               *sql.Stmt
	stmtUpsertAccountMetadata       *sql.Stmt
	stmtDeleteAccountMetadata       *sql.Stmt
	stmtInsertTransactionID         *sql.Stmt
	stmtGetLogIDForTransactionID    *sql.Stmt
	stmtInsertRevertedTransactionID *sql.Stmt
	stmtIsTransactionReverted       *sql.Stmt
	stmtInsertLedger                *sql.Stmt
	stmtListLedgers                 *sql.Stmt
}

// NewStore creates a new Store instance
func NewStore(db *DB, logger logging.Logger) (*Store, error) {
	s := &Store{
		db:     db,
		logger: logger,
	}

	ctx := context.Background()
	if err := s.createTables(ctx); err != nil {
		return nil, fmt.Errorf("creating tables: %w", err)
	}

	if err := s.prepareStatements(ctx); err != nil {
		return nil, err
	}

	return s, nil
}

// NewMattnStore creates a new SQLite Runtime store using github.com/mattn/go-sqlite3
func NewMattnStore(dsn string, logger logging.Logger) (*Store, error) {
	db, err := OpenMattnDB(dsn, otelsql.WithAttributes(
		attribute.String("store.type", "runtime"),
	))
	if err != nil {
		return nil, err
	}

	s, err := NewStore(db, logger)
	if err != nil {
		return nil, fmt.Errorf("creating runtime store: %w", err)
	}

	return s, nil
}

// NewModernStore creates a new SQLite Modern Runtime store
func NewModernStore(dsn string, logger logging.Logger) (*Store, error) {
	db, err := OpenModernDB(dsn, otelsql.WithAttributes(
		attribute.String("store.type", "runtime"),
	))
	if err != nil {
		return nil, err
	}

	s, err := NewStore(db, logger)
	if err != nil {
		return nil, fmt.Errorf("creating runtime store: %w", err)
	}

	return s, nil
}

func (s *Store) prepareStatements(ctx context.Context) error {
	var err error
	s.stmtGetLogByID, err = s.db.PrepareContext(ctx, `
		SELECT id, ledger, data, date, idempotency_key, idempotency_hash
		FROM logs WHERE ledger = ? AND id = ?
	`)
	if err != nil {
		return fmt.Errorf("preparing getLogByID statement: %w", err)
	}

	s.stmtInsertLog, err = s.db.PrepareContext(ctx, `
		INSERT INTO logs (ledger, data, date, idempotency_key, idempotency_hash, id)
		VALUES (?, ?, ?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("preparing insertLog statement: %w", err)
	}

	s.stmtGetIdempotency, err = s.db.PrepareContext(ctx, `
		SELECT id 
		FROM logs 
		WHERE ledger = ? AND idempotency_key = ?
	`)
	if err != nil {
		return fmt.Errorf("preparing getIdempotency statement: %w", err)
	}

	s.stmtInsertBalance, err = s.db.PrepareContext(ctx, `
		INSERT INTO balances (ledger, account, asset, balance) VALUES (?, ?, ?, ?)
		ON CONFLICT (ledger, account, asset) DO UPDATE SET balance = balance + excluded.balance
	`)
	if err != nil {
		return fmt.Errorf("preparing insertBalance statement: %w", err)
	}

	s.stmtUpsertAccountMetadata, err = s.db.PrepareContext(ctx, `
		INSERT OR REPLACE INTO account_metadata (ledger, account_address, key, value) VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("preparing upsertAccountMetadata statement: %w", err)
	}

	s.stmtDeleteAccountMetadata, err = s.db.PrepareContext(ctx, `
		DELETE FROM account_metadata WHERE ledger = ? AND account_address = ? AND key = ?
	`)
	if err != nil {
		return fmt.Errorf("preparing deleteAccountMetadata statement: %w", err)
	}

	s.stmtInsertTransactionID, err = s.db.PrepareContext(ctx, `
		INSERT OR REPLACE INTO transaction_ids (ledger, transaction_id, log_id) VALUES (?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("preparing insertTransactionID statement: %w", err)
	}

	s.stmtGetLogIDForTransactionID, err = s.db.PrepareContext(ctx, `
		SELECT log_id FROM transaction_ids WHERE ledger = ? AND transaction_id = ?
	`)
	if err != nil {
		return fmt.Errorf("preparing getLogIDForTransactionID statement: %w", err)
	}

	s.stmtInsertRevertedTransactionID, err = s.db.PrepareContext(ctx, `
		INSERT OR REPLACE INTO reverted_transaction_ids (ledger, transaction_id) VALUES (?, ?)
	`)
	if err != nil {
		return fmt.Errorf("preparing insertRevertedTransactionID statement: %w", err)
	}

	s.stmtIsTransactionReverted, err = s.db.PrepareContext(ctx, `
		SELECT 1 FROM reverted_transaction_ids WHERE ledger = ? AND transaction_id = ? LIMIT 1
	`)
	if err != nil {
		return fmt.Errorf("preparing isTransactionReverted statement: %w", err)
	}

	s.stmtInsertLedger, err = s.db.PrepareContext(ctx, `
		INSERT OR REPLACE INTO ledgers (id, name, metadata, created_at) VALUES (?, ?, ?, ?)
	`)
	if err != nil {
		return fmt.Errorf("preparing insertLedger statement: %w", err)
	}

	s.stmtListLedgers, err = s.db.PrepareContext(ctx, `
		SELECT id, name, metadata, created_at FROM ledgers ORDER BY id
	`)
	if err != nil {
		return fmt.Errorf("preparing listLedgers statement: %w", err)
	}

	return nil
}

func (s *Store) createTables(ctx context.Context) error {
	_, err := s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS ledgers (
			id INTEGER NOT NULL PRIMARY KEY,
			name TEXT NOT NULL UNIQUE,
			metadata TEXT,
			created_at TEXT
		);
	`)
	if err != nil {
		return fmt.Errorf("creating ledgers table: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS logs (
			ledger INTEGER NOT NULL, 
			id INTEGER NOT NULL, 
			data BLOB NOT NULL,
			date TEXT, 
			idempotency_key TEXT, 
			idempotency_hash TEXT,
			PRIMARY KEY (ledger, id), 
			UNIQUE(ledger, idempotency_key)
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS idx_logs_ledger_idempotency_key ON logs(ledger, idempotency_key);
	`)
	if err != nil {
		return fmt.Errorf("creating logs table: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS balances (
			ledger INTEGER NOT NULL, account TEXT NOT NULL, asset TEXT NOT NULL,
			balance TEXT NOT NULL DEFAULT '0', PRIMARY KEY (ledger, account, asset)
		) WITHOUT ROWID;
	`)
	if err != nil {
		return fmt.Errorf("creating balances table: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS account_metadata (
			ledger INTEGER NOT NULL, account_address TEXT NOT NULL, key TEXT NOT NULL,
			value TEXT NOT NULL, PRIMARY KEY (ledger, account_address, key)
		) WITHOUT ROWID;
	`)
	if err != nil {
		return fmt.Errorf("creating account_metadata table: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS transaction_ids (
			ledger INTEGER NOT NULL, 
			transaction_id INTEGER NOT NULL, 
			log_id INTEGER NOT NULL,
			PRIMARY KEY (ledger, transaction_id)
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS idx_transaction_ids_ledger_transaction_id ON transaction_ids(ledger, transaction_id);
	`)
	if err != nil {
		return fmt.Errorf("creating transaction_ids table: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS reverted_transaction_ids (
			ledger INTEGER NOT NULL, 
			transaction_id INTEGER NOT NULL, 
			PRIMARY KEY (ledger, transaction_id)
		) WITHOUT ROWID;
		CREATE INDEX IF NOT EXISTS idx_reverted_transaction_ids_ledger_transaction_id ON reverted_transaction_ids(ledger, transaction_id);
	`)
	if err != nil {
		return fmt.Errorf("creating reverted_transaction_ids table: %w", err)
	}

	_, err = s.db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS raft_state (
			key TEXT NOT NULL PRIMARY KEY,
			value INTEGER NOT NULL
		);
	`)
	if err != nil {
		return fmt.Errorf("creating raft_state table: %w", err)
	}

	return nil
}

// logCursor implements store.Cursor[*ledgerpb.Log] for SQLite.
type logCursor struct {
	rows *sql.Rows
	s    *Store
}

func (c *logCursor) Next(ctx context.Context) (*ledgerpb.Log, error) {
	if !c.rows.Next() {
		if err := c.rows.Err(); err != nil {
			return nil, err
		}
		return nil, io.EOF
	}

	var id sql.NullInt64
	var ledger uint32
	var dataBinary []byte
	var dateStr sql.NullString
	var idempotencyKey sql.NullString
	var idempotencyHash sql.Null[[]byte]

	err := c.rows.Scan(&id, &ledger, &dataBinary, &dateStr, &idempotencyKey, &idempotencyHash)
	if err != nil {
		return nil, fmt.Errorf("scanning log row: %w", err)
	}

	log := &ledgerpb.Log{LedgerId: ledger}
	if id.Valid {
		log.Id = uint64(id.Int64)
	}

	logPayload := &ledgerpb.LogPayload{}
	if err := proto.Unmarshal(dataBinary, logPayload); err != nil {
		return nil, fmt.Errorf("unmarshaling log payload from protobuf: %w", err)
	}
	log.Data = logPayload

	if dateStr.Valid {
		date, err := stdtime.Parse(stdtime.RFC3339, dateStr.String)
		if err != nil {
			return nil, fmt.Errorf("parsing date: %w", err)
		}
		log.Date = ledgerpb.NewTimestamp(time.New(date))
	}

	if idempotencyKey.Valid {
		log.Idempotency = &ledgerpb.Idempotency{Key: idempotencyKey.String, Hash: idempotencyHash.V}
	}

	return log, nil
}

func (c *logCursor) Close() error {
	if c.rows != nil {
		return c.rows.Close()
	}
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs for a specific ledger.
func (s *Store) GetAllLogs(ctx context.Context, ledger uint32, from uint64, to uint64) (store.Cursor[*ledgerpb.Log], error) {
	query := `SELECT id, ledger, data, date, idempotency_key, idempotency_hash FROM logs WHERE ledger = ?`
	args := []interface{}{ledger}
	if from > 0 {
		query += ` AND id > ?`
		args = append(args, int64(from))
	}
	if to > 0 {
		query += ` AND id <= ?`
		args = append(args, int64(to))
	}
	query += ` ORDER BY id ASC`

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying logs: %w", err)
	}

	return &logCursor{rows: rows, s: s}, nil
}

// GetLogByID retrieves a log by its ID for a specific ledger.
func (s *Store) GetLogByID(ctx context.Context, ledger uint32, id uint64) (*ledgerpb.Log, error) {
	row := s.stmtGetLogByID.QueryRowContext(ctx, ledger, id)

	var logID sql.NullInt64
	var logLedger uint32
	var dataBinary []byte
	var dateStr sql.NullString
	var idempotencyKey sql.NullString
	var idempotencyHash sql.Null[[]byte]

	err := row.Scan(&logID, &logLedger, &dataBinary, &dateStr, &idempotencyKey, &idempotencyHash)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("getting log by id: %w", err)
	}

	log := &ledgerpb.Log{LedgerId: logLedger}
	if logID.Valid {
		log.Id = uint64(logID.Int64)
	}

	logPayload := &ledgerpb.LogPayload{}
	if err := proto.Unmarshal(dataBinary, logPayload); err != nil {
		return nil, fmt.Errorf("unmarshaling log payload from protobuf: %w", err)
	}
	log.Data = logPayload

	if dateStr.Valid {
		date, err := stdtime.Parse(stdtime.RFC3339, dateStr.String)
		if err != nil {
			return nil, fmt.Errorf("parsing date: %w", err)
		}
		log.Date = ledgerpb.NewTimestamp(time.New(date))
	}

	if idempotencyKey.Valid {
		log.Idempotency = &ledgerpb.Idempotency{Key: idempotencyKey.String, Hash: idempotencyHash.V}
	}

	return log, nil
}

// Close closes the database connection and prepared statements
func (s *Store) Close(ctx context.Context) error {
	var errs []error
	for _, stmt := range []*sql.Stmt{
		s.stmtGetLogByID,
		s.stmtInsertLog,
		s.stmtGetIdempotency,
		s.stmtInsertBalance,
		s.stmtUpsertAccountMetadata,
		s.stmtDeleteAccountMetadata,
		s.stmtInsertTransactionID,
		s.stmtGetLogIDForTransactionID,
		s.stmtInsertRevertedTransactionID,
		s.stmtIsTransactionReverted,
		s.stmtInsertLedger,
		s.stmtListLedgers,
	} {
		if stmt != nil {
			if err := stmt.Close(); err != nil {
				errs = append(errs, err)
			}
		}
	}
	if err := s.db.Close(); err != nil {
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return fmt.Errorf("closing store: %v", errs)
	}
	return nil
}

// GetBalances retrieves balances from the balances table for a specific ledger
func (s *Store) GetBalances(ctx context.Context, ledger uint32, balanceQuery map[string][]string) (ledgerpb.Balances, error) {
	result := make(ledgerpb.Balances)

	for account, assets := range balanceQuery {
		if len(assets) == 0 {
			continue
		}
		result[account] = make(map[string]*big.Int)

		placeholders := make([]string, len(assets))
		args := make([]interface{}, len(assets)+2)
		args[0] = ledger
		args[1] = account
		for i, asset := range assets {
			placeholders[i] = "?"
			args[i+2] = asset
		}

		query := fmt.Sprintf(`SELECT asset, balance FROM balances WHERE ledger = ? AND account = ? AND asset IN (%s)`,
			strings.Join(placeholders, ", "))

		rows, err := s.db.QueryContext(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("querying balances: %w", err)
		}

		for rows.Next() {
			var asset, balanceStr string
			if err := rows.Scan(&asset, &balanceStr); err != nil {
				_ = rows.Close()
				return nil, fmt.Errorf("scanning balance row: %w", err)
			}
			balance, ok := new(big.Int).SetString(balanceStr, 10)
			if !ok {
				_ = rows.Close()
				return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
			}
			result[account][asset] = balance
		}
		_ = rows.Close()

		for _, asset := range assets {
			if _, exists := result[account][asset]; !exists {
				result[account][asset] = big.NewInt(0)
			}
		}
	}

	return result, nil
}

// GetAccountMetadata retrieves account metadata for multiple accounts
func (s *Store) GetAccountMetadata(ctx context.Context, ledger uint32, accounts []string) (map[string]metadata.Metadata, error) {
	result := make(map[string]metadata.Metadata)

	for _, account := range accounts {
		result[account] = make(metadata.Metadata)
	}

	placeholders := make([]string, len(accounts))
	args := make([]interface{}, len(accounts)+1)
	args[0] = ledger
	for i, account := range accounts {
		placeholders[i] = "?"
		args[i+1] = account
	}

	query := fmt.Sprintf(`SELECT account_address, key, value FROM account_metadata WHERE ledger = ? AND account_address IN (%s)`,
		strings.Join(placeholders, ", "))

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying account metadata: %w", err)
	}
	defer func() { _ = rows.Close() }()

	for rows.Next() {
		var address, key, valueJSON string
		if err := rows.Scan(&address, &key, &valueJSON); err != nil {
			return nil, fmt.Errorf("scanning account metadata row: %w", err)
		}

		var value interface{}
		if err := json.Unmarshal([]byte(valueJSON), &value); err != nil {
			return nil, fmt.Errorf("unmarshaling metadata value: %w", err)
		}

		if _, exists := result[address]; !exists {
			result[address] = make(metadata.Metadata)
		}
		if strVal, ok := value.(string); ok {
			result[address][key] = strVal
		} else {
			valJSON, _ := json.Marshal(value)
			result[address][key] = string(valJSON)
		}
	}

	return result, nil
}

// GetLogForIdempotencyKey retrieves the idempotency hash and the id of a log
func (s *Store) GetLogIDForIdempotencyKey(ctx context.Context, ledger uint32, idempotencyKey string) (uint64, error) {

	var logID uint64
	err := s.stmtGetIdempotency.QueryRowContext(ctx, ledger, idempotencyKey).Scan(&logID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("querying idempotency entry: %w", err)
	}
	return logID, nil
}

// GetLogIDForTransactionID retrieves the log ID for a given transaction ID
func (s *Store) GetLogIDForTransactionID(ctx context.Context, ledger uint32, transactionID uint64) (uint64, error) {

	var logID uint64
	err := s.stmtGetLogIDForTransactionID.QueryRowContext(ctx, ledger, transactionID).Scan(&logID)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, fmt.Errorf("querying transaction ID mapping: %w", err)
	}
	return logID, nil
}

// IsTransactionReverted checks if a transaction has been reverted
func (s *Store) IsTransactionReverted(ctx context.Context, ledger uint32, transactionID uint64) (bool, error) {

	var exists int
	err := s.stmtIsTransactionReverted.QueryRowContext(ctx, ledger, transactionID).Scan(&exists)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return false, nil
		}
		return false, fmt.Errorf("querying reverted transaction ID: %w", err)
	}
	return exists == 1, nil
}

func (s *Store) CreateSnapshot(ctx context.Context) error {
	return nil
}

// GetLastAppliedIndex retrieves the last applied Raft index.
func (s *Store) GetLastAppliedIndex() (uint64, error) {
	row := s.db.QueryRow(`SELECT value FROM raft_state WHERE key = 'last_applied_index'`)

	var lastAppliedIndex uint64
	if err := row.Scan(&lastAppliedIndex); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("querying last applied index: %w", err)
	}
	return lastAppliedIndex, nil
}

// GetLastLogID retrieves the ID of the last log for a specific ledger.
func (s *Store) GetLastLogID(ctx context.Context, ledger uint32) (uint64, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id FROM logs WHERE ledger = ? ORDER BY id DESC LIMIT 1`, ledger)

	var lastLogID uint64
	if err := row.Scan(&lastLogID); err != nil {
		if err == sql.ErrNoRows {
			return 0, nil
		}
		return 0, fmt.Errorf("querying last log ID: %w", err)
	}
	return lastLogID, nil
}

// GetLedgerByName retrieves a ledger by its name.
func (s *Store) GetLedgerByName(ctx context.Context, name string) (*ledgerpb.LedgerInfo, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, name, metadata, created_at FROM ledgers WHERE name = ?`, name)

	var id uint32
	var ledgerName string
	var metadataJSON sql.NullString
	var createdAtStr sql.NullString

	if err := row.Scan(&id, &ledgerName, &metadataJSON, &createdAtStr); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("querying ledger by name: %w", err)
	}

	info := &ledgerpb.LedgerInfo{
		Id:   id,
		Name: ledgerName,
	}

	if metadataJSON.Valid && metadataJSON.String != "" {
		var meta map[string]string
		if err := json.Unmarshal([]byte(metadataJSON.String), &meta); err != nil {
			return nil, fmt.Errorf("unmarshaling ledger metadata: %w", err)
		}
		info.Metadata = meta
	}

	if createdAtStr.Valid {
		createdAt, err := stdtime.Parse(stdtime.RFC3339, createdAtStr.String)
		if err != nil {
			return nil, fmt.Errorf("parsing created_at: %w", err)
		}
		info.CreatedAt = ledgerpb.NewTimestamp(time.New(createdAt))
	}

	return info, nil
}

// ListLedgers returns all registered ledgers.
func (s *Store) ListLedgers(ctx context.Context) ([]*ledgerpb.LedgerInfo, error) {
	rows, err := s.stmtListLedgers.QueryContext(ctx)
	if err != nil {
		return nil, fmt.Errorf("querying ledgers: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var ledgers []*ledgerpb.LedgerInfo
	for rows.Next() {
		var id uint32
		var name string
		var metadataJSON sql.NullString
		var createdAtStr sql.NullString

		if err := rows.Scan(&id, &name, &metadataJSON, &createdAtStr); err != nil {
			return nil, fmt.Errorf("scanning ledger row: %w", err)
		}

		info := &ledgerpb.LedgerInfo{
			Id:   id,
			Name: name,
		}

		if metadataJSON.Valid && metadataJSON.String != "" {
			var metadata map[string]string
			if err := json.Unmarshal([]byte(metadataJSON.String), &metadata); err != nil {
				return nil, fmt.Errorf("unmarshaling ledger metadata: %w", err)
			}
			info.Metadata = metadata
		}

		if createdAtStr.Valid {
			createdAt, err := stdtime.Parse(stdtime.RFC3339, createdAtStr.String)
			if err != nil {
				return nil, fmt.Errorf("parsing created_at: %w", err)
			}
			info.CreatedAt = ledgerpb.NewTimestamp(time.New(createdAt))
		}

		ledgers = append(ledgers, info)
	}

	return ledgers, nil
}
