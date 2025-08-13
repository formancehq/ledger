package bunconnect

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"net/url"
	"time"

	"github.com/uptrace/opentelemetry-go-extra/otelsql"

	"github.com/formancehq/go-libs/logging"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/extra/bunotel"
)

type ConnectionOptions struct {
	DatabaseSourceName string
	MaxIdleConns       int
	MaxOpenConns       int
	ConnMaxIdleTime    time.Duration
	Connector          func(dsn string) (driver.Connector, error) `json:",omitempty"`
}

func (opts ConnectionOptions) String() string {
	return fmt.Sprintf("dsn=%s, max-idle-conns=%d, max-open-conns=%d, conn-max-idle-time=%s",
		opts.DatabaseSourceName, opts.MaxIdleConns, opts.MaxOpenConns, opts.ConnMaxIdleTime)
}

func OpenSQLDB(ctx context.Context, options ConnectionOptions, hooks ...bun.QueryHook) (*bun.DB, error) {
	var (
		sqldb *sql.DB
		err   error
	)
	if options.Connector == nil {
		logging.FromContext(ctx).Debugf("Opening database with default connector and dsn: '%s'", options.DatabaseSourceName)
		sqldb, err = otelsql.Open("postgres", options.DatabaseSourceName)
		if err != nil {
			return nil, err
		}
	} else {
		logging.FromContext(ctx).Debugf("Opening database with connector and dsn: '%s'", options.DatabaseSourceName)
		connector, err := options.Connector(options.DatabaseSourceName)
		if err != nil {
			return nil, err
		}
		sqldb = otelsql.OpenDB(connector)
	}
	sqldb.SetMaxIdleConns(options.MaxIdleConns)
	if options.ConnMaxIdleTime != 0 {
		sqldb.SetConnMaxIdleTime(options.ConnMaxIdleTime)
	}
	if options.MaxOpenConns != 0 {
		sqldb.SetMaxOpenConns(options.MaxOpenConns)
	}

	db := bun.NewDB(sqldb, pgdialect.New(), bun.WithDiscardUnknownColumns())
	db.AddQueryHook(bunotel.NewQueryHook(bunotel.WithFormattedQueries(true)))
	for _, hook := range hooks {
		db.AddQueryHook(hook)
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func OpenDBWithSchema(ctx context.Context, connectionOptions ConnectionOptions, schema string, hooks ...bun.QueryHook) (*bun.DB, error) {
	parsedConnectionParams, err := url.Parse(connectionOptions.DatabaseSourceName)
	if err != nil {
		return nil, err
	}

	query := parsedConnectionParams.Query()
	query.Set("search_path", fmt.Sprintf(`"%s"`, schema))
	parsedConnectionParams.RawQuery = query.Encode()

	connectionOptions.DatabaseSourceName = parsedConnectionParams.String()

	return OpenSQLDB(ctx, connectionOptions, hooks...)
}
