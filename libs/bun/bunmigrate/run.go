package bunmigrate

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/formancehq/stack/libs/go-libs/bun/bunconnect"
	sharedlogging "github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/formancehq/stack/libs/go-libs/pointer"
	"github.com/formancehq/stack/libs/go-libs/service"
	"github.com/pkg/errors"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/extra/bundebug"
	"github.com/xo/dburl"
	"io"
)

func isDatabaseExists(ctx context.Context, db *bun.DB, name string) (bool, error) {
	row := db.QueryRowContext(ctx, `SELECT datname FROM pg_database WHERE datname = ?`, name)
	if row.Err() != nil {
		return false, errors.Wrap(row.Err(), "checking database list")
	}

	if err := row.Scan(pointer.For("")); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return false, errors.Wrap(err, "scanning database row")
		}

		return false, nil
	}

	return true, nil
}

func onPostgresDB(ctx context.Context, connectionOptions bunconnect.ConnectionOptions, callback func(db *bun.DB) error) error {
	url, err := dburl.Parse(connectionOptions.DatabaseSourceName)
	if err != nil {
		return errors.Wrapf(err, "parsing dsn: %s", connectionOptions.DatabaseSourceName)
	}

	url.Path = "postgres" // notes(gfyrag): default "postgres" database (most of the time?)
	connectionOptions.DatabaseSourceName = url.String()

	db, err := bunconnect.OpenSQLDB(ctx, connectionOptions)
	if err != nil {
		return errors.Wrap(err, "opening database")
	}
	defer func() {
		err := db.Close()
		if err != nil {
			sharedlogging.FromContext(ctx).Errorf("Closing database: %s", err)
		}
	}()

	return callback(db)
}

func EnsureDatabaseNotExists(ctx context.Context, connectionOptions bunconnect.ConnectionOptions) error {
	return onPostgresDB(ctx, connectionOptions, func(db *bun.DB) error {

		url, err := dburl.Parse(connectionOptions.DatabaseSourceName)
		if err != nil {
			return errors.Wrapf(err, "parsing dsn: %s", connectionOptions.DatabaseSourceName)
		}

		databaseExists, err := isDatabaseExists(ctx, db, url.Path[1:])
		if err != nil {
			return errors.Wrap(err, "checking if database exists")
		}

		if databaseExists {
			_, err = db.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE "%s"`, url.Path[1:]))
			if err != nil {
				return errors.Wrap(err, "dropping database")
			}
		}

		return nil
	})
}

func EnsureDatabaseExists(ctx context.Context, connectionOptions bunconnect.ConnectionOptions) error {
	return onPostgresDB(ctx, connectionOptions, func(db *bun.DB) error {

		url, err := dburl.Parse(connectionOptions.DatabaseSourceName)
		if err != nil {
			return errors.Wrapf(err, "parsing dsn: %s", connectionOptions.DatabaseSourceName)
		}

		databaseExists, err := isDatabaseExists(ctx, db, url.Path[1:])
		if err != nil {
			return errors.Wrap(err, "checking if database exists")
		}

		if !databaseExists {
			_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, url.Path[1:]))
			if err != nil {
				return errors.Wrap(err, "creating database")
			}
		}

		return nil
	})
}

func run(ctx context.Context, output io.Writer, args []string, connectionOptions *bunconnect.ConnectionOptions,
	executor func(args []string, db *bun.DB) error) error {

	if err := EnsureDatabaseExists(ctx, *connectionOptions); err != nil {
		return err
	}

	db, err := bunconnect.OpenSQLDB(ctx, *connectionOptions)
	if err != nil {
		return errors.Wrap(err, "opening database")
	}
	defer func() {
		_ = db.Close()
	}()
	if viper.GetBool(service.DebugFlag) {
		db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithWriter(output)))
	}

	return errors.Wrap(executor(args, db), "executing migration")
}

func Run(cmd *cobra.Command, args []string, executor Executor) error {
	connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(cmd.Context())
	if err != nil {
		return errors.Wrap(err, "evaluating connection options")
	}
	return run(cmd.Context(), cmd.OutOrStdout(), args, connectionOptions, func(args []string, db *bun.DB) error {
		return executor(cmd, args, db)
	})
}
