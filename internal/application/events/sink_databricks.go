//go:build databricks

package events

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/databricks/databricks-sql-go"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func init() {
	registerSinkFactory("databricks", func(sc *commonpb.SinkConfig, _ Format) (Sink, error) {
		s := sc.GetType().(*commonpb.SinkConfig_Databricks)

		port := int(s.Databricks.GetPort())
		if port == 0 {
			port = 443
		}

		return NewDatabricksSink(context.Background(), DatabricksSinkConfig{
			ServerHostname: s.Databricks.GetServerHostname(),
			HTTPPath:       s.Databricks.GetHttpPath(),
			Token:          s.Databricks.GetToken(),
			Catalog:        s.Databricks.GetCatalog(),
			Schema:         s.Databricks.GetSchema(),
			Table:          s.Databricks.GetTable(),
			Port:           port,
		})
	})
}

const defaultDatabricksTable = "ledger_events"

// DatabricksCreateTableDDL returns the CREATE TABLE statement for the events
// Delta table. The data column uses STRING to hold JSON — Databricks can query
// it with from_json() or the : JSON path operator.
func DatabricksCreateTableDDL(qualifiedTable string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
    log_sequence BIGINT,
    type         STRING,
    ledger       STRING,
    date         TIMESTAMP,
    data         STRING
) USING DELTA`, qualifiedTable)
}

// DatabricksSinkConfig holds configuration for the Databricks sink.
type DatabricksSinkConfig struct {
	ServerHostname string
	HTTPPath       string
	Token          string
	Catalog        string
	Schema         string
	Table          string
	Port           int
}

// DatabricksSink publishes events to a Databricks Delta table via SQL Warehouse.
type DatabricksSink struct {
	db             *sql.DB
	qualifiedTable string
}

// NewDatabricksSink creates a new Databricks sink, connects to the SQL Warehouse,
// and auto-creates the target Delta table.
func NewDatabricksSink(ctx context.Context, cfg DatabricksSinkConfig) (*DatabricksSink, error) {
	dsn := fmt.Sprintf("token:%s@%s:%d%s?catalog=%s&schema=%s",
		cfg.Token, cfg.ServerHostname, cfg.Port, cfg.HTTPPath, cfg.Catalog, cfg.Schema,
	)

	db, err := sql.Open("databricks", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening Databricks connection: %w", err)
	}

	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("pinging Databricks SQL Warehouse: %w", err)
	}

	table := cfg.Table
	if table == "" {
		table = defaultDatabricksTable
	}

	qualifiedTable := fmt.Sprintf("%s.%s.%s", cfg.Catalog, cfg.Schema, table)

	ddl := DatabricksCreateTableDDL(qualifiedTable)
	if _, err := db.ExecContext(ctx, ddl); err != nil {
		return nil, fmt.Errorf("creating Databricks table %s: %w", qualifiedTable, err)
	}

	return &DatabricksSink{
		db:             db,
		qualifiedTable: qualifiedTable,
	}, nil
}

func (s *DatabricksSink) Publish(ctx context.Context, events []*eventspb.Event) error {
	if len(events) == 0 {
		return nil
	}

	var (
		sb   strings.Builder
		args = make([]any, 0, len(events)*5)
	)

	sb.WriteString("INSERT INTO ")
	sb.WriteString(s.qualifiedTable)
	sb.WriteString(" (log_sequence, type, ledger, date, data) VALUES ")

	for i, event := range events {
		data, err := eventToSinkJSON(event)
		if err != nil {
			return fmt.Errorf("serializing event seq=%d: %w", event.GetLogSequence(), err)
		}

		eventType := strings.ToLower(event.GetType().String())
		eventDate := event.GetDate().AsTime().Time

		if i > 0 {
			sb.WriteString(", ")
		}

		sb.WriteString("(?, ?, ?, ?, ?)")

		args = append(args, event.GetLogSequence(), eventType, event.GetLedger(), eventDate, string(data))
	}

	if _, err := s.db.ExecContext(ctx, sb.String(), args...); err != nil {
		return fmt.Errorf("inserting events into Databricks: %w", err)
	}

	return nil
}

func (s *DatabricksSink) Close() error {
	return s.db.Close()
}
