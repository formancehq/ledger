package service

import (
	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/go-libs/v3/pointer"

	"context"
	"encoding/json"
	"fmt"
	"io"
	stdtime "time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	libtime "github.com/formancehq/go-libs/v3/time"
	ledger "github.com/formancehq/ledger-v3-poc/internal"
)

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

	return nil
}

// Close closes the database connection
func (s *ClickHouseLogStore) Close() error {
	return s.db.Close()
}

// InsertLogs inserts logs into the ClickHouse database (implements LogWriter)
func (s *ClickHouseLogStore) InsertLogs(ctx context.Context, logs ...ledger.Log) error {
	if len(logs) == 0 {
		return nil
	}

	// Use batch insert for better performance
	// Column order matches the table definition: ledger, id, type, date, data
	batch, err := s.db.PrepareBatch(ctx, "INSERT INTO logs (ledger, id, type, date, data, idempotency_key, idempotency_hash)")
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
		SELECT id, type, data, date, ledger, idempotency_hash
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
	)

	if err := row.Scan(&id, &logType, &dataJSON, &date, &ledgerNameResult, &idempotencyHash); err != nil {
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
	}, nil
}

// GetLastLog retrieves the last log by ID for a specific ledger (implements LogReader)
func (s *ClickHouseLogStore) GetLastLog(ctx context.Context, ledgerName string) (*ledger.Log, error) {
	// Use FINAL to get the latest version from ReplacingMergeTree
	rows, err := s.db.Query(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
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
	)

	if err := rows.Scan(&id, &logType, &dataJSON, &date, &ledgerNameResult, &idempotencyKey, &idempotencyHash); err != nil {
		return nil, fmt.Errorf("scanning log row: %w", err)
	}

	return s.scanLog(id, logType, dataJSON, date, ledgerNameResult, idempotencyKey, idempotencyHash)
}

// scanLog scans row data into a Log struct
func (s *ClickHouseLogStore) scanLog(id int64, logType string, dataJSON string, date stdtime.Time, ledgerName string, key string, hash string) (*ledger.Log, error) {
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
	)

	if err := c.rows.Scan(&id, &logType, &dataJSON, &date, &ledgerName, &idempotencyKey, &idempotencyHash); err != nil {
		return ledger.Log{}, fmt.Errorf("scanning log row: %w", err)
	}

	log, err := c.store.scanLog(id, logType, dataJSON, date, ledgerName, idempotencyKey, idempotencyHash)
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

// GetAllLogs returns a cursor to iterate over all logs for a specific ledger (implements LogReader)
// Logs are returned in ascending order by ID
func (s *ClickHouseLogStore) GetAllLogs(ctx context.Context, ledgerName string) (*Cursor[ledger.Log], error) {
	// Use FINAL to get the latest version from ReplacingMergeTree
	rows, err := s.db.Query(ctx, `
		SELECT id, type, data, date, ledger, idempotency_key, idempotency_hash
		FROM logs FINAL
		WHERE ledger = ?
		ORDER BY id ASC
	`, ledgerName)
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
