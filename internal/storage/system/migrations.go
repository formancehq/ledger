package system

import (
	"context"
	"database/sql"

	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/pkg/features"

	"github.com/formancehq/go-libs/v3/migrations"
	"github.com/uptrace/bun"
)

func GetMigrator(db bun.IDB, options ...migrations.Option) *migrations.Migrator {

	// configuration table has been removed, we keep the model to keep migrations consistent but the table is not used anymore.
	type configuration struct {
		bun.BaseModel `bun:"_system.configuration,alias:configuration"`

		Key     string    `bun:"key,type:varchar(255),pk"`
		Value   string    `bun:"value,type:text"`
		AddedAt time.Time `bun:"addedAt,type:timestamp"`
	}

	options = append(options, migrations.WithSchema(SchemaSystem))

	migrator := migrations.NewMigrator(db, options...)
	migrator.RegisterMigrations(
		migrations.Migration{
			Name: "Init schema",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					create table _system.ledgers (
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
				})
			},
		},
		migrations.Migration{
			Name: "Add ledger, bucket naming constraints 63 chars",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						alter column ledger type varchar(63),
						alter column bucket type varchar(63);
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add ledger metadata",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						add column if not exists metadata jsonb;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Fix empty ledger metadata",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						update _system.ledgers
						set metadata = '{}'::jsonb
						where metadata is null;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add ledger state",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						add column if not exists state varchar(255) default 'initializing';
	
						update _system.ledgers
						set state = 'in-use'
						where state = '';
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add features column",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					alter table _system.ledgers
					add column if not exists features jsonb;
				`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Rename ledger column to name",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					alter table _system.ledgers
					rename column ledger to name;
				`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add sequential id on ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create sequence _system.ledger_sequence;
							
						alter table _system.ledgers 
						add column id bigint default nextval('_system.ledger_sequence');
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add aggregate_objects pg aggregator",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, aggregateObjects)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Remove ledger state column",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						drop column state;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Remove configuration table",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						drop table _system.configuration;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Generate addedat of table ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					alter table _system.ledgers
					alter column addedat type timestamp without time zone;

					alter table _system.ledgers
					alter column addedat set default (now() at time zone 'utc');

					alter table _system.ledgers
					rename column addedat to added_at;
				`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "add pgcrypto",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create extension if not exists pgcrypto
						with schema public;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Configure features for old ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					update _system.ledgers
					set features = ?
					where features is null;
				`, features.DefaultFeatures)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add state column to ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
					alter table _system.ledgers
					add column state varchar(255) default 'initializing';
				`, features.DefaultFeatures)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "set default metadata on ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						alter column metadata set default '{}'::jsonb;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Create buckets table",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						create table _system.buckets (
							id serial primary key,
							name varchar(255) unique not null,
							added_at timestamp without time zone default (now() at time zone 'utc'),
							deleted_at timestamp
						);
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Populate buckets table from existing ledgers",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						insert into _system.buckets (name, added_at, deleted_at)
						select distinct bucket, min(added_at), null
						from _system.ledgers
						group by bucket
						on conflict (name) do nothing;
					`)
					return err
				})
			},
		},
		migrations.Migration{
			Name: "Add foreign key from ledgers to buckets",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, `
						alter table _system.ledgers
						add constraint fk_ledgers_bucket_buckets_name
						foreign key (bucket)
						references _system.buckets(name)
						on delete restrict;
					`)
					return err
				})
			},
		},
	)

	return migrator
}

func Migrate(ctx context.Context, db *bun.DB, options ...migrations.Option) error {
	return GetMigrator(db, options...).Up(ctx)
}

const aggregateObjects = `
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
