package bunconnect

import (
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"net/url"
	"time"

	"github.com/formancehq/stack/libs/go-libs/bun/bundebug"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/extra/bunotel"
)

type ConnectionOptions struct {
	DatabaseSourceName string
	Debug              bool
	Writer             io.Writer
	MaxIdleConns       int
	MaxOpenConns       int
	ConnMaxIdleTime    time.Duration
	Connector          func(dsn string) (driver.Connector, error) `json:",omitempty"`
}

func (opts ConnectionOptions) String() string {
	return fmt.Sprintf("dsn=%s, debug=%v, max-idle-conns=%d, max-open-conns=%d, conn-max-idle-time=%s",
		opts.DatabaseSourceName, opts.Debug, opts.MaxIdleConns, opts.MaxOpenConns, opts.ConnMaxIdleTime)
}

func OpenSQLDB(options ConnectionOptions, hooks ...bun.QueryHook) (*bun.DB, error) {
	var (
		sqldb *sql.DB
		err   error
	)
	if options.Connector == nil {
		sqldb, err = sql.Open("postgres", options.DatabaseSourceName)
		if err != nil {
			return nil, err
		}
	} else {
		connector, err := options.Connector(options.DatabaseSourceName)
		if err != nil {
			return nil, err
		}
		sqldb = sql.OpenDB(connector)
	}
	if options.MaxIdleConns != 0 {
		sqldb.SetMaxIdleConns(options.MaxIdleConns)
	}
	if options.ConnMaxIdleTime != 0 {
		sqldb.SetConnMaxIdleTime(options.ConnMaxIdleTime)
	}
	if options.MaxOpenConns != 0 {
		sqldb.SetMaxOpenConns(options.MaxOpenConns)
	}

	db := bun.NewDB(sqldb, pgdialect.New(), bun.WithDiscardUnknownColumns())
	if options.Debug {
		db.AddQueryHook(bundebug.NewQueryHook())
	}
	db.AddQueryHook(bunotel.NewQueryHook())
	for _, hook := range hooks {
		db.AddQueryHook(hook)
	}

	if err := db.Ping(); err != nil {
		return nil, err
	}

	return db, nil
}

func OpenDBWithSchema(connectionOptions ConnectionOptions, schema string, hooks ...bun.QueryHook) (*bun.DB, error) {
	parsedConnectionParams, err := url.Parse(connectionOptions.DatabaseSourceName)
	if err != nil {
		return nil, err
	}

	query := parsedConnectionParams.Query()
	query.Set("search_path", fmt.Sprintf(`"%s"`, schema))
	parsedConnectionParams.RawQuery = query.Encode()

	connectionOptions.DatabaseSourceName = parsedConnectionParams.String()

	return OpenSQLDB(connectionOptions, hooks...)
}
