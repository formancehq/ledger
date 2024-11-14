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

// stateless version (+1 regarding directory name, as migrations start from 1 in the lib)
const MinimalSchemaVersion = 12

type Bucket struct {
	name string
	db   *bun.DB
}

func (b *Bucket) Migrate(ctx context.Context, tracer trace.Tracer, minimalVersionReached chan struct{}, options ...migrations.Option) error {
	return migrate(ctx, tracer, b.db, b.name, minimalVersionReached, options...)
}

func (b *Bucket) HasMinimalVersion(ctx context.Context) (bool, error) {
	migrator := GetMigrator(b.db, b.name)
	lastVersion, err := migrator.GetLastVersion(ctx)
	if err != nil {
		return false, err
	}

	return lastVersion >= MinimalSchemaVersion, nil
}

func (b *Bucket) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return GetMigrator(b.db, b.name).GetMigrations(ctx)
}

func (b *Bucket) AddLedger(ctx context.Context, l ledger.Ledger, db bun.IDB) error {

	for _, setup := range ledgerSetups {
		if l.Features.Match(setup.requireFeatures) {
			tpl := template.Must(template.New("sql").Parse(setup.script))
			buf := bytes.NewBuffer(nil)
			if err := tpl.Execute(buf, l); err != nil {
				return fmt.Errorf("executing template: %w", err)
			}

			_, err := db.ExecContext(ctx, buf.String())
			if err != nil {
				return fmt.Errorf("executing sql: %w", err)
			}
		}
	}

	return nil
}

func New(db *bun.DB, name string) *Bucket {
	return &Bucket{
		db:   db,
		name: name,
	}
}

type ledgerSetup struct {
	requireFeatures ledger.FeatureSet
	script string
}

var ledgerSetups = []ledgerSetup{
	{
		script: `
		-- create a sequence for transactions by ledger instead of a sequence of the table as we want to have contiguous ids
		-- notes: we can still have "holes" on ids since a sql transaction can be reverted after a usage of the sequence
		create sequence "{{.Bucket}}"."transaction_id_{{.ID}}" owned by "{{.Bucket}}".transactions.id;
		select setval('"{{.Bucket}}"."transaction_id_{{.ID}}"', coalesce((
			select max(id) + 1
			from "{{.Bucket}}".transactions
			where ledger = '{{ .Name }}'
		), 1)::bigint, false);
		`,
	},
	{
		script: `
		-- create a sequence for logs by ledger instead of a sequence of the table as we want to have contiguous ids
		-- notes: we can still have "holes" on id since a sql transaction can be reverted after a usage of the sequence
		create sequence "{{.Bucket}}"."log_id_{{.ID}}" owned by "{{.Bucket}}".logs.id;
		select setval('"{{.Bucket}}"."log_id_{{.ID}}"', coalesce((
			select max(id) + 1
			from "{{.Bucket}}".logs
			where ledger = '{{ .Name }}'
		), 1)::bigint, false);
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureMovesHistoryPostCommitEffectiveVolumes: "SYNC",
		},
		script: `create index "pcev_{{.ID}}" on "{{.Bucket}}".moves (accounts_address, asset, effective_date desc) where ledger = '{{.Name}}';`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureMovesHistoryPostCommitEffectiveVolumes: "SYNC",
		},
		script: `
		create trigger "set_effective_volumes_{{.ID}}"
		before insert
		on "{{.Bucket}}"."moves"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".set_effective_volumes();
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureMovesHistoryPostCommitEffectiveVolumes: "SYNC",
		},
		script: `
		create trigger "update_effective_volumes_{{.ID}}"
		after insert
		on "{{.Bucket}}"."moves"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".update_effective_volumes();
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureHashLogs: "SYNC",
		},
		script: `
		create trigger "set_log_hash_{{.ID}}"
		before insert
		on "{{.Bucket}}"."logs"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".set_log_hash();
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureAccountMetadataHistory: "SYNC",
		},
		script: `
		create trigger "update_account_metadata_history_{{.ID}}"
		after update
		on "{{.Bucket}}"."accounts"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".update_account_metadata_history();
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureAccountMetadataHistory: "SYNC",
		},
		script: `
		create trigger "insert_account_metadata_history_{{.ID}}"
		after insert
		on "{{.Bucket}}"."accounts"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".insert_account_metadata_history();
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureTransactionMetadataHistory: "SYNC",
		},
		script: `
		create trigger "update_transaction_metadata_history_{{.ID}}"
		after update
		on "{{.Bucket}}"."transactions"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".update_transaction_metadata_history();
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureTransactionMetadataHistory: "SYNC",
		},
		script: `
		create trigger "insert_transaction_metadata_history_{{.ID}}"
		after insert
		on "{{.Bucket}}"."transactions"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".insert_transaction_metadata_history();
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureIndexTransactionAccounts: "SYNC",
		},
		script: `
		create index "transactions_sources_{{.ID}}" on "{{.Bucket}}".transactions using gin (sources jsonb_path_ops) where ledger = '{{.Name}}';
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureIndexTransactionAccounts: "ON",
		},
		script: `
		create index "transactions_destinations_{{.ID}}" on "{{.Bucket}}".transactions using gin (destinations jsonb_path_ops) where ledger = '{{.Name}}';
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureIndexTransactionAccounts: "ON",
		},
		script: `
		create trigger "transaction_set_addresses_{{.ID}}"
		before insert
		on "{{.Bucket}}"."transactions"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".set_transaction_addresses();	
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureIndexAddressSegments: "ON",
		},
		script: `
		create index "accounts_address_array_{{.ID}}" on "{{.Bucket}}".accounts using gin (address_array jsonb_ops) where ledger = '{{.Name}}';
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureIndexAddressSegments: "ON",
		},
		script: `
		create index "accounts_address_array_length_{{.ID}}" on "{{.Bucket}}".accounts (jsonb_array_length(address_array)) where ledger = '{{.Name}}';
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureIndexAddressSegments: "ON",
		},
		script: `
		create trigger "accounts_set_address_array_{{.ID}}"
		before insert
		on "{{.Bucket}}"."accounts"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".set_address_array_for_account();
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureIndexAddressSegments: "ON",
			ledger.FeatureIndexTransactionAccounts: "ON",
		},
		script: `
		create index "transactions_sources_arrays_{{.ID}}" on "{{.Bucket}}".transactions using gin (sources_arrays jsonb_path_ops) where ledger = '{{.Name}}';
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureIndexAddressSegments: "ON",
			ledger.FeatureIndexTransactionAccounts: "ON",
		},
		script: `
		create index "transactions_destinations_arrays_{{.ID}}" on "{{.Bucket}}".transactions using gin (destinations_arrays jsonb_path_ops) where ledger = '{{.Name}}';
		`,
	},
	{
		requireFeatures: ledger.FeatureSet{
			ledger.FeatureIndexAddressSegments: "ON",
			ledger.FeatureIndexTransactionAccounts: "ON",
		},
		script: `
		create trigger "transaction_set_addresses_segments_{{.ID}}"
		before insert
		on "{{.Bucket}}"."transactions"
		for each row
		when (
			new.ledger = '{{.Name}}'
		)
		execute procedure "{{.Bucket}}".set_transaction_addresses_segments();
		`,
	},
}