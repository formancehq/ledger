package service

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"

	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/big"
	"strings"
	stdtime "time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	libtime "github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

// ClickHouseConfig represents the configuration for a ClickHouse bucket driver
type ClickHouseConfig struct {
	DSN string `json:"dsn"` // Data Source Name (connection string)
}

// ClickHouseLogStore is a ClickHouse implementation of LogStore
type ClickHouseLogStore struct {
	db     driver.Conn
	logger logging.Logger
}

// NewClickHouseLogStore creates a new ClickHouse log store
func NewClickHouseLogStore(ctx context.Context, dsn string, logger logging.Logger) (*ClickHouseLogStore, error) {
	// Parse DSN and open ClickHouse connection
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("parsing clickhouse dsn: %w", err)
	}

	// Configure ClickHouse settings
	options.Settings = map[string]any{
		"date_time_input_format":                    "best_effort",
		"date_time_output_format":                   "iso",
		"allow_experimental_dynamic_type":           true,
		"enable_json_type":                          true,
		"enable_variant_type":                       true,
		"output_format_json_quote_64bit_integers":   false,
		"output_format_native_write_json_as_string": true,
	}

	db, err := clickhouse.Open(options)
	if err != nil {
		return nil, fmt.Errorf("opening clickhouse database: %w", err)
	}

	// Test the connection
	if err := db.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging clickhouse database: %w", err)
	}

	store := &ClickHouseLogStore{
		db:     db,
		logger: logger,
	}

	// Create tables if they don't exist
	if err := store.createTables(ctx); err != nil {
		return nil, fmt.Errorf("creating tables: %w", err)
	}

	return store, nil
}

// createTables creates the necessary tables for logs
// Based on the table definition from github.com/formancehq/ledger exporters
func (s *ClickHouseLogStore) createTables(ctx context.Context) error {
	// Create logs table with ReplacingMergeTree engine
	// Following the same structure as the ledger repository exporters
	err := s.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS logs (
			ledger String,
			id              Int64,
			type            String,
			date            DateTime64(6, 'UTC'),
			idempotency_key String,
			idempotency_hash String,
			sequence        UInt64 NOT NULL DEFAULT 0,
			data            JSON(
				transaction JSON(
					id UInt256,
					insertedAt DateTime64(6, 'UTC'),
					postings Array(JSON(
						source String,
						destination String,
						amount UInt256,
						asset String
					)),
					metadata Map(String, String),
					reference String,
					reverted Bool,
					timestamp DateTime64(6, 'UTC')
				),
				accountMetadata Map(String, Map(String, String)),
				targetId Variant(UInt256, String),
				targetType Nullable(String),
				metadata Map(String, String),
				key Nullable(String),
				revertedTransaction JSON(
					id UInt256,
					insertedAt DateTime64(6, 'UTC'),
					postings Array(JSON(
						source String,
						destination String,
						amount UInt256,
						asset String
					)),
					metadata Map(String, String),
					reference String,
					reverted Bool,
					timestamp DateTime64(6, 'UTC')
				)
			)
		)
		ENGINE = ReplacingMergeTree
		PARTITION BY ledger
		PRIMARY KEY (ledger, id);
	`)
	if err != nil {
		return fmt.Errorf("creating logs table: %w", err)
	}

	// Create balances table (SummingMergeTree for automatic aggregation)
	err = s.db.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS balances (
			ledger String,
			account String,
			asset String,
			balance Int256
		)
		ENGINE = SummingMergeTree(balance)
		PARTITION BY ledger
		PRIMARY KEY (ledger, account, asset);
	`)
	if err != nil {
		return fmt.Errorf("creating balances table: %w", err)
	}

	// Create a single materialized view that handles both NEW_TRANSACTION and REVERTED_TRANSACTION
	// For NEW_TRANSACTION: source gets negative, destination gets positive
	// For REVERTED_TRANSACTION: destination gets negative (reverse), source gets positive (reverse)
	// Using JSONExtract with proper path syntax for nested JSON arrays
	err = s.db.Exec(ctx, `
		CREATE MATERIALIZED VIEW IF NOT EXISTS balances_mv
		TO balances
		AS
		SELECT
			ledger,
			account,
			asset,
			balance
		FROM (
			-- NEW_TRANSACTION: source accounts (negative)
			SELECT
				ledger,
				JSONExtractString(posting_raw, 'source') AS account,
				JSONExtractString(posting_raw, 'asset') AS asset,
				-toInt256(JSONExtractString(posting_raw, 'amount')) AS balance
			FROM logs
			ARRAY JOIN JSONExtractArrayRaw(toString(data), 'transaction', 'postings') AS posting_raw
			WHERE type = 'NEW_TRANSACTION' OR type = 'REVERTED_TRANSACTION'
			
			UNION ALL
			
			-- NEW_TRANSACTION: destination accounts (positive)
			SELECT
				ledger,
				JSONExtractString(posting_raw, 'destination') AS account,
				JSONExtractString(posting_raw, 'asset') AS asset,
				toInt256(JSONExtractString(posting_raw, 'amount')) AS balance
			FROM logs
			ARRAY JOIN JSONExtractArrayRaw(toString(data), 'transaction', 'postings') AS posting_raw
			WHERE type = 'NEW_TRANSACTION' OR type = 'REVERTED_TRANSACTION'
		)
		WHERE account != '' AND asset != '';
	`)
	if err != nil {
		return fmt.Errorf("creating balances_mv: %w", err)
	}

	return nil
}

// Close closes the database connection
func (s *ClickHouseLogStore) Close() error {
	return s.db.Close()
}

// InsertLogs inserts logs into the ClickHouse database (implements LogWriter)
// The sequence number must be provided by the application (already set in log.Sequence)
func (s *ClickHouseLogStore) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// Use batch insert for better performance
	// Column order matches the table definition: ledger, id, type, date, data, sequence
	// Note: sequence is provided by the application, not calculated here
	batch, err := s.db.PrepareBatch(ctx, "INSERT INTO logs (ledger, id, type, date, data, idempotency_key, idempotency_hash, sequence)")
	if err != nil {
		return fmt.Errorf("preparing batch insert: %w", err)
	}

	for _, log := range logs {
		// Marshal data to JSON
		dataJSON, err := json.Marshal(log.Data)
		if err != nil {
			return fmt.Errorf("marshaling log data: %w", err)
		}

		// Format date as DateTime64(6, 'UTC')
		// ClickHouse expects dates in UTC format: "2006-01-02 15:04:05.999999 +00:00"
		var dateStr string
		if !log.Date.IsZero() {
			dateStr = log.Date.Format("2006-01-02 15:04:05.999999") + " +00:00"
		}

		// Get ID value
		var id int64
		if log.ID != nil {
			id = int64(*log.ID)
		}

		// Append to batch
		if err := batch.Append(
			log.Ledger,
			id,
			log.Type.String(),
			dateStr,
			string(dataJSON),
			log.IdempotencyKey,
			log.IdempotencyHash,
			int64(log.Sequence),
		); err != nil {
			return fmt.Errorf("appending log to batch: %w", err)
		}
	}

	// Send batch
	if err := batch.Send(); err != nil {
		return fmt.Errorf("sending batch: %w", err)
	}

	s.logger.WithFields(map[string]any{"count": len(logs)}).Debugf("Logs inserted into ClickHouse")
	return nil
}

// GetLogWithIdempotencyKey retrieves a log by its idempotency key (implements LogReader)
// Note: Since the table doesn't have idempotency_key column, we need to scan logs
// and check the idempotency key from the log structure. This is not efficient but matches the table structure.
func (s *ClickHouseLogStore) GetLogWithIdempotencyKey(ctx context.Context, ledgerName string, idempotencyKey string) (*ledger.Log, error) {
	// Use FINAL to get the latest version from ReplacingMergeTree
	row := s.db.QueryRow(ctx, `
		SELECT id, type, data, date, ledger, idempotency_hash, sequence
		FROM logs FINAL
		WHERE ledger = ? AND idempotency_key = ?
		ORDER BY id ASC
	`, ledgerName, idempotencyKey)
	if row.Err() != nil {
		return nil, fmt.Errorf("querying log by idempotency key: %w", row.Err())
	}
	var (
		id               int64
		logType          string
		dataJSON         string
		date             stdtime.Time
		ledgerNameResult string
		idempotencyHash  string
		sequence         uint64
	)

	if err := row.Scan(&id, &logType, &dataJSON, &date, &ledgerNameResult, &idempotencyHash, &sequence); err != nil {
		return nil, fmt.Errorf("scanning log row: %w", err)
	}

	// Parse the log to check idempotency key
	logTypeEnum := ledger.LogTypeFromString(logType)
	logData, err := ledger.HydrateLog(logTypeEnum, []byte(dataJSON))
	if err != nil {
		return nil, err
	}

	return &ledger.Log{
		ID:              pointer.For(uint64(id)),
		Type:            logTypeEnum,
		Data:            logData,
		Ledger:          ledgerNameResult,
		IdempotencyKey:  idempotencyKey,
		IdempotencyHash: idempotencyHash,
		Sequence:        sequence,
	}, nil
}

// GetLastLog retrieves the last log by ID for a specific ledger (implements LogReader)
func (s *ClickHouseLogStore) GetLastLog(ctx context.Context, ledgerName string) (*ledger.Log, error) {
	// Use FINAL to get the latest version from ReplacingMergeTree
	rows, err := s.db.Query(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash, sequence
		FROM logs FINAL
		WHERE ledger = ?
		ORDER BY id DESC
		LIMIT 1
	`, ledgerName)
	if err != nil {
		return nil, fmt.Errorf("querying last log: %w", err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, nil
	}

	var (
		id               int64
		logType          string
		dataJSON         string
		date             stdtime.Time
		ledgerNameResult string
		idempotencyKey   string
		idempotencyHash  string
		sequence         uint64
	)

	if err := rows.Scan(&id, &logType, &dataJSON, &date, &ledgerNameResult, &idempotencyKey, &idempotencyHash, &sequence); err != nil {
		return nil, fmt.Errorf("scanning log row: %w", err)
	}

	return s.scanLog(id, logType, dataJSON, date, ledgerNameResult, idempotencyKey, idempotencyHash, sequence)
}

// scanLog scans row data into a Log struct
func (s *ClickHouseLogStore) scanLog(id int64, logType string, dataJSON string, date stdtime.Time, ledgerName string, key string, hash string, sequence uint64) (*ledger.Log, error) {
	log := &ledger.Log{}

	// Set ID
	log.ID = pointer.For(uint64(id))

	// Set type
	log.Type = ledger.LogTypeFromString(logType)

	// Unmarshal data using HydrateLog
	var err error
	log.Data, err = ledger.HydrateLog(log.Type, []byte(dataJSON))
	if err != nil {
		return nil, fmt.Errorf("hydrating log data: %w", err)
	}

	// Set date
	if !date.IsZero() {
		log.Date = libtime.New(date)
	}

	// Set ledger
	log.Ledger = ledgerName
	log.IdempotencyKey = key
	log.IdempotencyHash = hash
	log.Sequence = sequence

	return log, nil
}

// clickHouseLogCursor implements Cursor[ledger.Log] for ClickHouse
type clickHouseLogCursor struct {
	rows  driver.Rows
	store *ClickHouseLogStore
}

func (c *clickHouseLogCursor) Next(ctx context.Context) (ledger.Log, error) {
	if !c.rows.Next() {
		if err := c.rows.Err(); err != nil {
			return ledger.Log{}, err
		}
		return ledger.Log{}, io.EOF
	}

	var (
		id              int64
		logType         string
		dataJSON        string
		date            stdtime.Time
		ledgerName      string
		idempotencyKey  string
		idempotencyHash string
		sequence        uint64
	)

	if err := c.rows.Scan(&id, &logType, &dataJSON, &date, &ledgerName, &idempotencyKey, &idempotencyHash, &sequence); err != nil {
		return ledger.Log{}, fmt.Errorf("scanning log row: %w", err)
	}

	log, err := c.store.scanLog(id, logType, dataJSON, date, ledgerName, idempotencyKey, idempotencyHash, sequence)
	if err != nil {
		return ledger.Log{}, err
	}

	return *log, nil
}

func (c *clickHouseLogCursor) Close() error {
	if c.rows != nil {
		return c.rows.Close()
	}
	return nil
}

// GetAllLogs returns a cursor to iterate over all logs (implements LogReader)
// Logs are returned in ascending order by sequence
// from: optional sequence number to start from (0 = from beginning)
func (s *ClickHouseLogStore) GetAllLogs(ctx context.Context, from uint64, to uint64) (*Cursor[ledger.Log], error) {
	// Use FINAL to get the latest version from ReplacingMergeTree
	query := `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash, sequence
		FROM logs FINAL
	`
	args := []interface{}{}
	whereClauses := []string{}
	if from > 0 {
		whereClauses = append(whereClauses, `sequence >= ?`)
		args = append(args, int64(from))
	}
	if to > 0 {
		whereClauses = append(whereClauses, `sequence <= ?`)
		args = append(args, int64(to))
	}
	if len(whereClauses) > 0 {
		query += ` WHERE ` + whereClauses[0]
		for i := 1; i < len(whereClauses); i++ {
			query += ` AND ` + whereClauses[i]
		}
	}
	query += ` ORDER BY sequence ASC`

	rows, err := s.db.Query(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("querying logs: %w", err)
	}

	cursor := &clickHouseLogCursor{
		rows:  rows,
		store: s,
	}

	var cursorInterface Cursor[ledger.Log] = cursor
	return &cursorInterface, nil
}

// GetLastSequenceID returns the highest sequence number in the logs table (implements LogWriter)
func (s *ClickHouseLogStore) GetLastSequenceID(ctx context.Context) (uint64, error) {
	var maxSeq uint64
	err := s.db.QueryRow(ctx, `SELECT MAX(sequence) FROM logs FINAL`).Scan(&maxSeq)
	if err != nil {
		return 0, fmt.Errorf("querying max sequence: %w", err)
	}
	return maxSeq, nil
}

// GetBalances retrieves balances from the balances table (implements BalancesStore)
func (s *ClickHouseLogStore) GetBalances(ctx context.Context, ledgerName string, balanceQuery map[string][]string) (ledger.Balances, error) {
	result := make(ledger.Balances)

	// If no query provided, return empty balances
	if len(balanceQuery) == 0 {
		return result, nil
	}

	// Build query for each account/asset combination
	for account, assets := range balanceQuery {
		if len(assets) == 0 {
			continue
		}

		// Initialize account map
		result[account] = make(map[string]*big.Int)

		// Build placeholders for IN clause (ClickHouse uses ?)
		placeholders := make([]string, len(assets))
		args := make([]interface{}, len(assets)+2)
		args[0] = ledgerName
		args[1] = account

		for i, asset := range assets {
			placeholders[i] = "?"
			args[i+2] = asset
		}

		query := fmt.Sprintf(`
			SELECT asset, toString(sum(balance)) FROM balances FINAL
			WHERE ledger = ? AND account = ? AND asset IN (%s)
			GROUP BY asset
		`, strings.Join(placeholders, ", "))

		rows, err := s.db.Query(ctx, query, args...)
		if err != nil {
			return nil, fmt.Errorf("querying balances: %w", err)
		}
		defer rows.Close()

		for rows.Next() {
			var asset string
			var balanceStr string
			if err := rows.Scan(&asset, &balanceStr); err != nil {
				return nil, fmt.Errorf("scanning balance row: %w", err)
			}

			if balanceStr == "" {
				result[account][asset] = big.NewInt(0)
				continue
			}

			balance, ok := new(big.Int).SetString(balanceStr, 10)
			if !ok {
				return nil, fmt.Errorf("invalid balance string: %s", balanceStr)
			}
			result[account][asset] = balance
		}

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("iterating balance rows: %w", err)
		}

		// Set zero balance for assets that don't exist in the database
		for _, asset := range assets {
			if _, exists := result[account][asset]; !exists {
				result[account][asset] = big.NewInt(0)
			}
		}
	}

	return result, nil
}
