//go:build databricks

package events

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"strings"

	dbsql "github.com/databricks/databricks-sql-go"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/eventspb"
)

func init() {
	registerSinkFactory("databricks", func(sc *commonpb.SinkConfig, _ Format) (Sink, error) {
		s := sc.GetType().(*commonpb.SinkConfig_Databricks)

		cfg, err := databricksConfigFromProto(s.Databricks)
		if err != nil {
			return nil, err
		}

		return NewDatabricksSink(context.Background(), cfg)
	})
}

// databricksConfigFromProto maps the protobuf Databricks sink config to the
// internal config. The auth oneof selects PAT (token) or OAuth M2M; a non-nil
// but fully empty OAuth M2M message is rejected here so the operator does not
// silently fall back to a "no auth configured" error further down the stack.
// The port default (443) is applied by newDatabricksConnector to keep a single
// source of truth.
func databricksConfigFromProto(pb *commonpb.DatabricksSinkConfig) (DatabricksSinkConfig, error) {
	cfg := DatabricksSinkConfig{
		ServerHostname: pb.GetServerHostname(),
		HTTPPath:       pb.GetHttpPath(),
		Catalog:        pb.GetCatalog(),
		Schema:         pb.GetSchema(),
		Table:          pb.GetTable(),
		Port:           int(pb.GetPort()),
	}

	switch a := pb.GetAuth().(type) {
	case *commonpb.DatabricksSinkConfig_Token:
		cfg.Token = a.Token
	case *commonpb.DatabricksSinkConfig_OauthM2M:
		m2m := a.OauthM2M
		if m2m == nil || (m2m.GetClientId() == "" && m2m.GetClientSecret() == "") {
			return DatabricksSinkConfig{}, errors.New("databricks: oauth_m2m is set but client_id and client_secret are both empty — provide both")
		}

		cfg.OAuthClientID = m2m.GetClientId()
		cfg.OAuthClientSecret = m2m.GetClientSecret()
	}

	return cfg, nil
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
// Authentication is mutually exclusive: set Token for PAT auth, or set both
// OAuthClientID and OAuthClientSecret for OAuth M2M (service principal) auth.
type DatabricksSinkConfig struct {
	ServerHostname    string
	HTTPPath          string
	Token             string // PAT — mutually exclusive with OAuth fields
	OAuthClientID     string // OAuth M2M client ID — mutually exclusive with Token
	OAuthClientSecret string // OAuth M2M client secret — mutually exclusive with Token
	Catalog           string
	Schema            string
	Table             string
	Port              int
}

// DatabricksSink publishes events to a Databricks Delta table via SQL Warehouse.
type DatabricksSink struct {
	db             *sql.DB
	qualifiedTable string
}

// newDatabricksConnector builds a driver.Connector from cfg, validating that
// exactly one auth method is configured (PAT or OAuth M2M).
func newDatabricksConnector(cfg DatabricksSinkConfig) (driver.Connector, error) {
	hasPAT := cfg.Token != ""
	hasOAuth := cfg.OAuthClientID != "" || cfg.OAuthClientSecret != ""

	switch {
	case hasPAT && hasOAuth:
		return nil, errors.New("databricks: token and oauth_client_id/oauth_client_secret are mutually exclusive — set exactly one auth method")
	case !hasPAT && !hasOAuth:
		return nil, errors.New("databricks: no authentication configured — set either token (PAT) or oauth_client_id + oauth_client_secret (OAuth M2M)")
	case hasOAuth && (cfg.OAuthClientID == "" || cfg.OAuthClientSecret == ""):
		return nil, errors.New("databricks: OAuth M2M requires both oauth_client_id and oauth_client_secret")
	}

	port := cfg.Port
	if port == 0 {
		port = 443
	}

	opts := []dbsql.ConnOption{
		dbsql.WithServerHostname(cfg.ServerHostname),
		dbsql.WithHTTPPath(cfg.HTTPPath),
		dbsql.WithPort(port),
		dbsql.WithInitialNamespace(cfg.Catalog, cfg.Schema),
	}

	if hasPAT {
		opts = append(opts, dbsql.WithAccessToken(cfg.Token))
	} else {
		opts = append(opts, dbsql.WithClientCredentials(cfg.OAuthClientID, cfg.OAuthClientSecret))
	}

	return dbsql.NewConnector(opts...)
}

// NewDatabricksSink creates a new Databricks sink, connects to the SQL Warehouse,
// and auto-creates the target Delta table.
//
// Authentication: exactly one of PAT (Token) or OAuth M2M (OAuthClientID +
// OAuthClientSecret) must be configured.
func NewDatabricksSink(ctx context.Context, cfg DatabricksSinkConfig) (*DatabricksSink, error) {
	connector, err := newDatabricksConnector(cfg)
	if err != nil {
		return nil, err
	}

	db := sql.OpenDB(connector)

	// Deferred close covers every error path between OpenDB and the successful
	// return. Without this any post-Open failure (Ping, DDL, future steps)
	// leaks the connection pool plus the OAuth token-refresh goroutines.
	success := false

	defer func() {
		if !success {
			_ = db.Close()
		}
	}()

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

	success = true

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
		eventDate := event.DateTs().AsTime().Time

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
