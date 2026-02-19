package events

import (
	"context"
	"encoding/hex"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
)

const defaultClickHouseTable = "ledger_events"

// ClickHouseSinkConfig holds configuration for the ClickHouse sink.
type ClickHouseSinkConfig struct {
	DSN    string
	Table  string
	Format Format
}

// ClickHouseSink publishes events to a ClickHouse table.
type ClickHouseSink struct {
	conn   driver.Conn
	table  string
	format Format
}

// NewClickHouseSink creates a new ClickHouse sink, connects, and auto-creates the target table.
func NewClickHouseSink(ctx context.Context, cfg ClickHouseSinkConfig) (*ClickHouseSink, error) {
	opts, err := clickhouse.ParseDSN(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parsing ClickHouse DSN: %w", err)
	}

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

	ddl := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    log_sequence UInt64,
    type         LowCardinality(String),
    ledger       LowCardinality(String),
    date         DateTime64(6, 'UTC'),
    data         String
) ENGINE = MergeTree()
ORDER BY (ledger, log_sequence)`, table)

	if err := conn.Exec(ctx, ddl); err != nil {
		return nil, fmt.Errorf("creating ClickHouse table %s: %w", table, err)
	}

	return &ClickHouseSink{
		conn:   conn,
		table:  table,
		format: cfg.Format,
	}, nil
}

func (s *ClickHouseSink) Publish(ctx context.Context, events []*eventspb.Event) error {
	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", s.table))
	if err != nil {
		return fmt.Errorf("preparing ClickHouse batch: %w", err)
	}

	for _, event := range events {
		raw, err := SerializeEvent(event, s.format)
		if err != nil {
			return fmt.Errorf("serializing event seq=%d: %w", event.LogSequence, err)
		}

		// Store protobuf as hex-encoded string for safe text storage
		data := string(raw)
		if s.format == FormatProto {
			data = hex.EncodeToString(raw)
		}

		eventType := strings.ToLower(event.Type.String())
		eventDate := event.Date.AsTime().Time

		if err := batch.Append(
			event.LogSequence,
			eventType,
			event.Ledger,
			eventDate,
			data,
		); err != nil {
			return fmt.Errorf("appending event seq=%d to batch: %w", event.LogSequence, err)
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
