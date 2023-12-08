package systemstore

import (
	"context"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/pkg/errors"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/formancehq/stack/libs/go-libs/migrations"
	"github.com/uptrace/bun"
)

func Migrate(ctx context.Context, db bun.IDB) error {
	migrator := migrations.NewMigrator(migrations.WithSchema(Schema, true))
	migrator.RegisterMigrations(
		migrations.Migration{
			Name: "Init schema",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {

				logging.FromContext(ctx).Infof("Checking if ledger v1 upgrade")
				exists, err := tx.NewSelect().
					TableExpr("information_schema.columns").
					Where("table_name = 'ledgers'").
					Exists(ctx)
				if err != nil {
					return err
				}

				if exists {
					logging.FromContext(ctx).Infof("Detect ledger v1 installation, trigger migration")
					_, err := tx.NewAddColumn().
						Table("ledgers").
						ColumnExpr("bucket varchar(255)").
						Exec(ctx)

					return errors.Wrap(err, "adding 'bucket' column")
				}

				_, err = tx.NewCreateTable().
					Model((*Ledger)(nil)).
					IfNotExists().
					Exec(ctx)
				if err != nil {
					return sqlutils.PostgresError(err)
				}

				_, err = tx.NewCreateTable().
					Model((*configuration)(nil)).
					IfNotExists().
					Exec(ctx)
				return sqlutils.PostgresError(err)
			},
		},
	)
	return migrator.Up(ctx, db)
}
