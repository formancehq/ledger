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

func ensureDatabaseExists(ctx context.Context, connectionOptions bunconnect.ConnectionOptions) error {
	url, err := dburl.Parse(connectionOptions.DatabaseSourceName)
	if err != nil {
		return err
	}
	originalPath := url.Path
	url.Path = "postgres" // notes(gfyrag): default "postgres" database (most of the time?)
	connectionOptions.DatabaseSourceName = url.String()

	db, err := bunconnect.OpenSQLDB(connectionOptions)
	if err != nil {
		return errors.Wrap(err, "opening database")
	}
	defer func() {
		err := db.Close()
		if err != nil {
			sharedlogging.FromContext(ctx).Errorf("Closing database: %s", err)
		}
	}()

	row := db.QueryRowContext(ctx, `SELECT datname FROM pg_database WHERE datname = ?`, originalPath[1:])
	if row.Err() != nil {
		return row.Err()
	}

	if err := row.Scan(pointer.For("")); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}

		_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE "%s"`, originalPath[1:]))
		if err != nil {
			return err
		}
	}

	return nil
}

func run(ctx context.Context, output io.Writer, args []string, connectionOptions *bunconnect.ConnectionOptions,
	executor func(args []string, db *bun.DB) error) error {

	if err := ensureDatabaseExists(ctx, *connectionOptions); err != nil {
		return err
	}

	db, err := bunconnect.OpenSQLDB(*connectionOptions)
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
	connectionOptions, err := bunconnect.ConnectionOptionsFromFlags(viper.GetViper(), cmd.OutOrStdout(), viper.GetBool(service.DebugFlag))
	if err != nil {
		return errors.Wrap(err, "evaluating connection options")
	}
	return run(cmd.Context(), cmd.OutOrStdout(), args, connectionOptions, func(args []string, db *bun.DB) error {
		return executor(cmd, args, db)
	})
}
