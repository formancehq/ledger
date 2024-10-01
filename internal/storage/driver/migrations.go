package driver

import (
	"context"
	"github.com/formancehq/go-libs/time"

	"github.com/formancehq/go-libs/platform/postgres"

	"github.com/formancehq/go-libs/migrations"
	"github.com/uptrace/bun"
)

func GetMigrator() *migrations.Migrator {

	// configuration table has been removed, we keep the model to keep migrations consistent but the table is now removed
	type configuration struct {
		bun.BaseModel `bun:"_system.configuration,alias:configuration"`

		Key     string    `bun:"key,type:varchar(255),pk"`
		Value   string    `bun:"value,type:text"`
		AddedAt time.Time `bun:"addedAt,type:timestamp"`
	}

	migrator := migrations.NewMigrator(migrations.WithSchema(SchemaSystem, true))
	migrator.RegisterMigrations(
		migrations.Migration{
			Name: "Init schema",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					create table ledgers (
						ledger varchar primary key,
						addedat timestamp,
						bucket varchar(255)
					)
				`)
				if err != nil {
					return err
				}

				_, err = tx.NewCreateTable().
					Model((*configuration)(nil)).
					Exec(ctx)
				return postgres.ResolveError(err)
			},
		},
		migrations.Migration{
			Name: "Add ledger, bucket naming constraints 63 chars",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					alter table ledgers
					alter column ledger type varchar(63),
					alter column bucket type varchar(63);
				`)
				return err
			},
		},
		migrations.Migration{
			Name: "Add ledger metadata",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					alter table ledgers
					add column if not exists metadata jsonb;
				`)
				return err
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
				return err
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
				return err
			},
		},
		migrations.Migration{
			Name: "Add features column",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					alter table ledgers
					add column if not exists features jsonb;
				`)
				return err
			},
		},
		migrations.Migration{
			Name: "Rename ledger column to name",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					alter table ledgers
					rename column ledger to name;
				`)
				return err
			},
		},
		migrations.Migration{
			Name: "Add sequential id on ledgers",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					create sequence ledger_sequence;
						
					alter table ledgers 
					add column id varchar default nextval('ledger_sequence');
				`)
				return err
			},
		},
		migrations.Migration{
			Name: "Add aggregate_objects pg aggregator",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, jsonbMerge)
				return err
			},
		},
		migrations.Migration{
			Name: "Remove ledger state column",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					alter table _system.ledgers
					drop column state;
				`)
				return err
			},
		},
		migrations.Migration{
			Name: "Remove configuration table",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.ExecContext(ctx, `
					drop table _system.configuration;
				`)
				return err
			},
		},
	)

	return migrator
}

func Migrate(ctx context.Context, db bun.IDB) error {
	return GetMigrator().Up(ctx, db)
}

const jsonbMerge = `
create or replace function public.jsonb_concat(a jsonb, b jsonb) returns jsonb
    as 'select $1 || $2'
    language sql
    immutable
    parallel safe
;

create or replace aggregate public.aggregate_objects(jsonb)
(
    sfunc = public.jsonb_concat,
    stype = jsonb,
    initcond = '{}'
);
`
