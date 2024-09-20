package systemstore

import (
	"context"

	"github.com/formancehq/go-libs/logging"
	"github.com/pkg/errors"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/formancehq/go-libs/migrations"
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
					if err != nil {
						return errors.Wrap(err, "adding 'bucket' column")
					}
					_, err = tx.NewUpdate().
						Table("ledgers").
						Set("bucket = ledger").
						Where("1 = 1").
						Exec(ctx)
					return errors.Wrap(err, "setting 'bucket' column")
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
		migrations.Migration{
			Name: "Add ledger, bucket naming constraints 63 chars",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					alter table ledgers
					add column if not exists ledger varchar(63),
					add column if not exists bucket varchar(63);

					alter table ledgers
					alter column ledger type varchar(63),
					alter column bucket type varchar(63);
				`)
				if err != nil {
					return err
				}
				return nil
			},
		},
		migrations.Migration{
			Name: "Add ledger metadata",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					alter table ledgers
					add column if not exists metadata jsonb;
				`)
				if err != nil {
					return err
				}
				return nil
			},
		},
		migrations.Migration{
			Name: "Fix empty ledger metadata",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					update ledgers
					set metadata = '{}'::jsonb
					where metadata is null;
				`)
				if err != nil {
					return err
				}
				return nil
			},
		},
		migrations.Migration{
			Name: "Add ledger state",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					alter table ledgers
					add column if not exists state varchar(255) default 'initializing';

					update ledgers
					set state = 'in-use'
					where state = '';
				`)
				if err != nil {
					return err
				}
				return nil
			},
		},
	)
	return migrator.Up(ctx, db)
}
