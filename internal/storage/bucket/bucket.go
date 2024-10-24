package bucket

import (
	"bytes"
	"context"
	_ "embed"
	"fmt"
	"github.com/formancehq/go-libs/v2/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"text/template"
)

// migration 18 (+1 regarding directory name, as migrations start from 1 in the lib)
const MinimalSchemaVersion = 19

type Bucket struct {
	name string
	db   bun.IDB
}

func (b *Bucket) Migrate(ctx context.Context, tracer trace.Tracer) error {
	return migrate(ctx, tracer, b.db, b.name)
}

func (b *Bucket) IsUpToDate(ctx context.Context) (bool, error) {
	migrator := GetMigrator(b.name)
	lastVersion, err := migrator.GetLastVersion(ctx, b.db)
	if err != nil {
		return false, err
	}

	return lastVersion >= MinimalSchemaVersion, nil
}

func (b *Bucket) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return GetMigrator(b.name).GetMigrations(ctx, b.db)
}

func (b *Bucket) AddLedger(ctx context.Context, l ledger.Ledger, db bun.IDB) error {

	tpl := template.Must(template.New("sql").Parse(addLedgerTpl))
	buf := bytes.NewBuffer(nil)
	if err := tpl.Execute(buf, l); err != nil {
		return fmt.Errorf("executing template: %w", err)
	}

	_, err := db.ExecContext(ctx, buf.String())
	if err != nil {
		return fmt.Errorf("executing sql: %w", err)
	}

	return nil
}

func New(db bun.IDB, name string) *Bucket {
	return &Bucket{
		db:   db,
		name: name,
	}
}

const addLedgerTpl = `
-- create a sequence for transactions by ledger instead of a sequence of the table as we want to have contiguous ids
-- notes: we can still have "holes" on ids since a sql transaction can be reverted after a usage of the sequence
create sequence "{{.Bucket}}"."transaction_id_{{.ID}}" owned by "{{.Bucket}}".transactions.id;
select setval('"{{.Bucket}}"."transaction_id_{{.ID}}"', coalesce((
    select max(id) + 1
    from "{{.Bucket}}".transactions
    where ledger = '{{ .Name }}'
), 1)::bigint, false);

-- create a sequence for logs by ledger instead of a sequence of the table as we want to have contiguous ids
-- notes: we can still have "holes" on id since a sql transaction can be reverted after a usage of the sequence
create sequence "{{.Bucket}}"."log_id_{{.ID}}" owned by "{{.Bucket}}".logs.id;
select setval('"{{.Bucket}}"."log_id_{{.ID}}"', coalesce((
    select max(id) + 1
    from "{{.Bucket}}".logs
    where ledger = '{{ .Name }}'
), 1)::bigint, false);

-- enable post commit effective volumes synchronously

{{ if .HasFeature "MOVES_HISTORY_POST_COMMIT_EFFECTIVE_VOLUMES" "SYNC" }}
create index "pcev_{{.ID}}" on "{{.Bucket}}".moves (accounts_address, asset, effective_date desc) where ledger = '{{.Name}}';

create trigger "set_effective_volumes_{{.ID}}"
before insert
on "{{.Bucket}}"."moves"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".set_effective_volumes();

create trigger "update_effective_volumes_{{.ID}}"
after insert
on "{{.Bucket}}"."moves"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".update_effective_volumes();
{{ end }}

-- logs hash

{{ if .HasFeature "HASH_LOGS" "SYNC" }}
create trigger "set_log_hash_{{.ID}}"
before insert
on "{{.Bucket}}"."logs"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".set_log_hash();
{{ end }}

{{ if .HasFeature "ACCOUNT_METADATA_HISTORY" "SYNC" }}
create trigger "update_account_metadata_history_{{.ID}}"
after update
on "{{.Bucket}}"."accounts"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".update_account_metadata_history();

create trigger "insert_account_metadata_history_{{.ID}}"
after insert
on "{{.Bucket}}"."accounts"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".insert_account_metadata_history();
{{ end }}

{{ if .HasFeature "TRANSACTION_METADATA_HISTORY" "SYNC" }}
create trigger "update_transaction_metadata_history_{{.ID}}"
after update
on "{{.Bucket}}"."transactions"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".update_transaction_metadata_history();

create trigger "insert_transaction_metadata_history_{{.ID}}"
after insert
on "{{.Bucket}}"."transactions"
for each row
when (
    new.ledger = '{{.Name}}'
)
execute procedure "{{.Bucket}}".insert_transaction_metadata_history();
{{ end }}

{{ if .HasFeature "INDEX_TRANSACTION_ACCOUNTS" "ON" }}
create index "transactions_sources_{{.ID}}" on "{{.Bucket}}".transactions using gin (sources jsonb_path_ops) where ledger = '{{.Name}}';
create index "transactions_destinations_{{.ID}}" on "{{.Bucket}}".transactions using gin (destinations jsonb_path_ops) where ledger = '{{.Name}}';
create trigger "transaction_set_addresses_{{.ID}}"
	before insert
	on "{{.Bucket}}"."transactions"
	for each row
	when (
		new.ledger = '{{.Name}}'
	)
execute procedure "{{.Bucket}}".set_transaction_addresses();
{{ end }}

{{ if .HasFeature "INDEX_ADDRESS_SEGMENTS" "ON" }}
create index "accounts_address_array_{{.ID}}" on "{{.Bucket}}".accounts using gin (address_array jsonb_ops) where ledger = '{{.Name}}';
create index "accounts_address_array_length_{{.ID}}" on "{{.Bucket}}".accounts (jsonb_array_length(address_array)) where ledger = '{{.Name}}';

create trigger "accounts_set_address_array_{{.ID}}"
	before insert
	on "{{.Bucket}}"."accounts"
	for each row
	when (
		new.ledger = '{{.Name}}'
	)
execute procedure "{{.Bucket}}".set_address_array_for_account();

{{ if .HasFeature "INDEX_TRANSACTION_ACCOUNTS" "ON" }}
create index "transactions_sources_arrays_{{.ID}}" on "{{.Bucket}}".transactions using gin (sources_arrays jsonb_path_ops) where ledger = '{{.Name}}';
create index "transactions_destinations_arrays_{{.ID}}" on "{{.Bucket}}".transactions using gin (destinations_arrays jsonb_path_ops) where ledger = '{{.Name}}';

create trigger "transaction_set_addresses_segments_{{.ID}}"
	before insert
	on "{{.Bucket}}"."transactions"
	for each row
	when (
		new.ledger = '{{.Name}}'
	)
execute procedure "{{.Bucket}}".set_transaction_addresses_segments();
{{ end }}
{{ end }}
`
