package clickhouse

import (
	"context"
	"encoding/json"
	"github.com/formancehq/ledger/internal/replication/config"
	"github.com/formancehq/ledger/internal/replication/drivers"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/pkg/errors"
)

type Connector struct {
	db     driver.Conn
	config Config
	logger logging.Logger
}

func (c *Connector) Stop(_ context.Context) error {
	return c.db.Close()
}

func (c *Connector) Start(ctx context.Context) error {

	var err error
	c.db, err = OpenDB(c.logger, c.config.DSN, false)
	if err != nil {
		return errors.Wrap(err, "opening database")
	}

	// Create the logs table
	// One table is used for the entire stack
	err = c.db.Exec(ctx, createLogsTable)
	if err != nil {
		return errors.Wrap(err, "failed to create logs table")
	}

	return nil
}

func (c *Connector) Accept(ctx context.Context, logs ...drivers.LogWithLedger) ([]error, error) {

	batch, err := c.db.PrepareBatch(ctx, "insert into logs(ledger, id, type, date, data)")
	if err != nil {
		return nil, errors.Wrap(err, "failed to prepare batch")
	}

	for _, log := range logs {

		data, err := json.Marshal(log.Data)
		if err != nil {
			return nil, errors.Wrap(err, "marshalling data")
		}

		if err := batch.Append(
			log.Ledger,
			*log.ID,
			log.Type,
			log.Date.Format(time.DateTime+".000000000"),
			string(data),
		); err != nil {
			return nil, errors.Wrap(err, "appending item to the batch")
		}
	}

	return make([]error, len(logs)), errors.Wrap(batch.Send(), "failed to commit transaction")
}

func NewConnector(config Config, logger logging.Logger) (*Connector, error) {
	return &Connector{
		config: config,
		logger: logger,
	}, nil
}

var _ drivers.Driver = (*Connector)(nil)

type Config struct {
	DSN string `json:"dsn"`
}

func (cfg Config) Validate() error {
	if cfg.DSN == "" {
		return errors.New("dsn is required")
	}

	return nil
}

var _ config.Validator = (*Config)(nil)

const createLogsTable = `
	create table if not exists logs (
		ledger String,
		id              Int64,
		type            String,
		date            DateTime64,
		data            JSON
	)
	engine = ReplacingMergeTree
	partition by ledger
	primary key (ledger, id);
`

func OpenDB(logger logging.Logger, dsn string, debug bool) (driver.Conn, error) {
	// Open database connection
	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return nil, errors.Wrap(err, "parsing dsn")
	}
	if debug {
		options.Debug = true
		options.Debugf = logger.Debugf
	}
	options.Settings = map[string]any{
		"allow_experimental_dynamic_type": true,
		"enable_json_type":                true,
	}
	// todo: make conditional
	// options.TLS = &tls.Config{}

	db, err := clickhouse.Open(options)
	if err != nil {
		return nil, errors.Wrap(err, "failed to open db")
	}

	return db, nil
}
