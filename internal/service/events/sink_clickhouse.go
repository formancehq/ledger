package events

import (
	"context"
	"fmt"
	"strings"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/formancehq/ledger-v3-poc/internal/proto/eventspb"
)

const defaultClickHouseTable = "ledger_events"

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
	for k, v := range clickhouseRequiredSettings {
		opts.Settings[k] = v
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

	if err := conn.Exec(ctx, ClickHouseCreateTableDDL(table)); err != nil {
		return nil, fmt.Errorf("creating ClickHouse table %s: %w", table, err)
	}

	return &ClickHouseSink{
		conn:  conn,
		table: table,
	}, nil
}

func (s *ClickHouseSink) Publish(ctx context.Context, events []*eventspb.Event) error {
	batch, err := s.conn.PrepareBatch(ctx, fmt.Sprintf("INSERT INTO %s", s.table))
	if err != nil {
		return fmt.Errorf("preparing ClickHouse batch: %w", err)
	}

	for _, event := range events {
		data, err := eventToClickHouseJSON(event)
		if err != nil {
			return fmt.Errorf("serializing event seq=%d: %w", event.LogSequence, err)
		}

		eventType := strings.ToLower(event.Type.String())
		eventDate := event.Date.AsTime().Time

		if err := batch.Append(
			event.LogSequence,
			eventType,
			event.Ledger,
			eventDate,
			string(data),
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
