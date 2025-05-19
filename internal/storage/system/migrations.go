package system

import (
	"context"
	"database/sql"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/time"
	"github.com/formancehq/ledger/pkg/features"

	"github.com/formancehq/go-libs/v2/migrations"
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
			Name: "Add json_compact function",
			Up: func(ctx context.Context, db bun.IDB) error {
				return db.RunInTx(ctx, &sql.TxOptions{}, func(ctx context.Context, tx bun.Tx) error {
					_, err := tx.ExecContext(ctx, jsonCompact)
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

const jsonCompact = `
CREATE OR REPLACE FUNCTION public.json_compact(p_json JSON,
                                               p_step INTEGER DEFAULT 0)
RETURNS JSON
AS $$
DECLARE
  v_type TEXT;
  v_text TEXT := '';
  v_indent INTEGER;
  v_key TEXT;
  v_object JSON;
  v_count INTEGER;
BEGIN
  p_step := coalesce(p_step, 0);
  -- Object or array?
  v_type := json_typeof(p_json);

  IF v_type = 'object' THEN
    -- Start object
    v_text := '{';
    SELECT count(*) - 1 INTO v_count
    FROM json_object_keys(p_json);
    -- go through keys, add them and recurse over value
    FOR v_key IN (SELECT json_object_keys(p_json))
    LOOP
      v_text := v_text || to_json(v_key)::TEXT || ':' || public.json_compact(p_json->v_key, p_step + 1);
      IF v_count > 0 THEN
        v_text := v_text || ',';
        v_count := v_count - 1;
      END IF;
      --v_text := v_text || E'\n';
    END LOOP;
    -- Close object
    v_text := v_text || '}';
  ELSIF v_type = 'array' THEN
    -- Start array
    v_text := '[';
    v_count := json_array_length(p_json) - 1;
    -- go through elements and add them through recursion
    FOR v_object IN (SELECT json_array_elements(p_json))
    LOOP
      v_text := v_text || public.json_compact(v_object, p_step + 1);
      IF v_count > 0 THEN
        v_text := v_text || ',';
        v_count := v_count - 1;
      END IF;
      --v_text := v_text || E'\n';
    END LOOP;
    -- Close array
    v_text := v_text || ']';
  ELSE -- A simple value
    v_text := v_text || p_json::TEXT;
  END IF;
  IF p_step > 0 THEN RETURN v_text;
  ELSE RETURN v_text::JSON;
  END IF;
END;
$$ LANGUAGE plpgsql;
`