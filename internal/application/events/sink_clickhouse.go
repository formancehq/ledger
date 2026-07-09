//go:build clickhouse

package events

import (
	"context"
	"fmt"
	"maps"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func init() {
	registerSinkFactory("clickhouse", func(sc *commonpb.SinkConfig, _ Format) (Sink, error) {
		s := sc.GetType().(*commonpb.SinkConfig_Clickhouse)

		return NewClickHouseSink(context.Background(), ClickHouseSinkConfig{
			DSN:   s.Clickhouse.GetDsn(),
			Table: s.Clickhouse.GetTable(),
		})
	})
}

const defaultClickHouseTable = "ledger_events"

// clickhouseTransactionColumns defines the typed sub-columns for a transaction
// inside the JSON column. Reused for both `transaction` and `revertTransaction`.
const clickhouseTransactionColumns = `JSON(
            id UInt64,
            postings Array(JSON(
                source String,
                destination String,
                amount UInt256,
                asset String
            )),
            metadata Map(String, String),
            reference Nullable(String),
            timestamp DateTime64(6, 'UTC'),
            reverted Bool,
            insertedAt DateTime64(6, 'UTC')
        )`

// ClickHouseCreateTableDDL returns the CREATE TABLE statement for the events table
// with a fully-typed JSON column matching the ledger v2 reference implementation.
//
// Delivery is at-least-once, so a redelivered batch re-inserts rows with an
// identical (ledger, log_sequence). ReplacingMergeTree collapses those duplicates
// during background merges; dedup is therefore eventual, and exact queries must
// use FINAL or GROUP BY to avoid transiently counting an un-merged duplicate.
func ClickHouseCreateTableDDL(table string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    log_sequence UInt64,
    type         LowCardinality(String),
    ledger       LowCardinality(String),
    date         DateTime64(6, 'UTC'),
    data         JSON(
        transaction %s,
        accountMetadata Map(String, String),
        revertedTransactionId Nullable(UInt64),
        revertTransaction %s,
        targetType Nullable(String),
        targetId Variant(UInt64, String),
        metadata Map(String, String),
        key Nullable(String),
        ledgerName Nullable(String),
        signingKeyId Nullable(String),
        publicKey Nullable(String),
        requireSignatures Nullable(Bool),
        sinkName Nullable(String),
        hash Nullable(String)
    )
) ENGINE = ReplacingMergeTree()
ORDER BY (ledger, log_sequence)`, table, clickhouseTransactionColumns, clickhouseTransactionColumns)
}

// clickhouseRequiredSettings are connection-level settings required for the
// structured JSON column, following the ledger v2 reference implementation.
// Uses "allow_experimental_*" names for ClickHouse 24.x compatibility;
// ClickHouse 25.x+ accepts both the experimental and non-experimental names.
var clickhouseRequiredSettings = clickhouse.Settings{
	"date_time_input_format":                  "best_effort",
	"allow_experimental_json_type":            true,
	"allow_experimental_variant_type":         true,
	"output_format_json_quote_64bit_integers": false,
}

// ClickHouseSinkConfig holds configuration for the ClickHouse sink.
type ClickHouseSinkConfig struct {
	DSN   string
	Table string
}

// ClickHouseSink publishes events to a ClickHouse table with a fully-typed
// JSON column. Events are always serialized as ClickHouse-friendly JSON
// (encoding/json with native Go types) — the Format field from SinkConfig
// is irrelevant for ClickHouse.
type ClickHouseSink struct {
	conn  driver.Conn
	table string
}

// NewClickHouseSink creates a new ClickHouse sink, connects, and auto-creates
// the target table with a structured JSON column.
func NewClickHouseSink(ctx context.Context, cfg ClickHouseSinkConfig) (*ClickHouseSink, error) {
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parsing ClickHouse DSN: %w", err)
	}

	// Merge required settings (don't override user-provided DSN settings)
	if opts.Settings == nil {
		opts.Settings = make(clickhouse.Settings)
	}

	maps.Copy(opts.Settings, clickhouseRequiredSettings)

	conn, err := clickhouse.Open(opts)
	if err != nil {
		return nil, fmt.Errorf("opening ClickHouse connection: %w", err)
	}

	if err := conn.Ping(ctx); err != nil {
		return nil, fmt.Errorf("pinging ClickHouse: %w", err)
	}

	table := cfg.Table
	if table == "" {
		table = defaultClickHouseTable
	}

	if err := conn.Exec(ctx, ClickHouseCreateTableDDL(table)); err != nil {
		return nil, fmt.Errorf("creating ClickHouse table %s: %w", table, err)
	}

	return &ClickHouseSink{
		conn:  conn,
		table: table,
	}, nil
}

func (s *ClickHouseSink) Publish(ctx context.Context, events []*eventspb.Event) error {
	batch, err := s.conn.PrepareBatch(ctx, "INSERT INTO "+s.table)
	if err != nil {
		return fmt.Errorf("preparing ClickHouse batch: %w", err)
	}

	for _, event := range events {
		data, err := eventToSinkJSON(event)
		if err != nil {
			return fmt.Errorf("serializing event seq=%d: %w", event.GetLogSequence(), err)
		}

		eventType := strings.ToLower(event.GetType().String())
		eventDate := event.GetDate().AsTime().Time

		if err := batch.Append(
			event.GetLogSequence(),
			eventType,
			event.GetLedger(),
			eventDate,
			string(data),
		); err != nil {
			return fmt.Errorf("appending event seq=%d to batch: %w", event.GetLogSequence(), err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("sending ClickHouse batch: %w", err)
	}

	return nil
}

func (s *ClickHouseSink) Close() error {
	return s.conn.Close()
}
