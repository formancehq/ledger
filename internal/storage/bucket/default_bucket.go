package bucket

import (
	"bytes"
	"context"
	_ "embed"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v3/migrations"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/features"
	"github.com/uptrace/bun"
	"go.opentelemetry.io/otel/trace"
	"text/template"
)

// stateless version (+1 regarding directory name, as migrations start from 1 in the lib)
const MinimalSchemaVersion = 26

type DefaultBucket struct {
	name   string

	tracer trace.Tracer
}

func (b *DefaultBucket) IsInitialized(ctx context.Context, db bun.IDB) (bool, error) {
	_, err := GetMigrator(db, b.name).GetLastVersion(ctx)
	if err == nil {
		return true, nil
	}
	if errors.Is(err, migrations.ErrMissingVersionTable) {
		return false, nil
	}
	return false, err
}

func (b *DefaultBucket) IsUpToDate(ctx context.Context, db bun.IDB) (bool, error) {
	return GetMigrator(db, b.name).IsUpToDate(ctx)
}

func (b *DefaultBucket) Migrate(ctx context.Context, db bun.IDB, options ...migrations.Option) error {
	return runMigrate(ctx, b.tracer, db, b.name, append(options, migrations.WithTracer(b.tracer))...)
}

func (b *DefaultBucket) HasMinimalVersion(ctx context.Context, db bun.IDB) (bool, error) {
	lastVersion, err := b.GetLastVersion(ctx, db)
	if err != nil {
		return false, err
	}

	return lastVersion >= MinimalSchemaVersion, nil
}

func (b *DefaultBucket) GetLastVersion(ctx context.Context, db bun.IDB) (int, error) {
	return GetMigrator(db, b.name).GetLastVersion(ctx)
}

func (b *DefaultBucket) GetMigrationsInfo(ctx context.Context, db bun.IDB) ([]migrations.Info, error) {
	return GetMigrator(db, b.name).GetMigrations(ctx)
}

func (b *DefaultBucket) AddLedger(ctx context.Context, db bun.IDB, l ledger.Ledger) error {

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

func NewDefault(tracer trace.Tracer, name string) *DefaultBucket {
	return &DefaultBucket{

		name:   name,
		tracer: tracer,
	}
}

type ledgerSetup struct {
	requireFeatures features.FeatureSet
	script          string
}

// notes: Be careful if changing the order of the migration.
// The actions on tables are organized following the same order used when committing a new transaction.
// It prevents deadlocks.
var ledgerSetups = []ledgerSetup{
	{
		script: `
		-- create a sequence for transactions by ledger instead of a sequence of the table as we want to have contiguous ids
		-- notes: we can still have "holes" on ids since a sql transaction can be reverted after a usage of the sequence
		create sequence "{{.Bucket}}"."transaction_id_{{.ID}}" owned by "{{.Bucket}}".transactions.id;
		select setval('"{{.Bucket}}"."transaction_id_{{.ID}}"', coalesce((
			select max(id) + 1
			from "{{.Bucket}}"."transactions"
			where ledger = '{{ .Name }}'
		), 1)::bigint, false);
		`,
	},
	{
		requireFeatures: features.FeatureSet{
			features.FeatureTransactionMetadataHistory: "SYNC",
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
		requireFeatures: features.FeatureSet{
			features.FeatureTransactionMetadataHistory: "SYNC",
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
		requireFeatures: features.FeatureSet{
			features.FeatureIndexTransactionAccounts: "ON",
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
		requireFeatures: features.FeatureSet{
			features.FeatureIndexAddressSegments:     "ON",
			features.FeatureIndexTransactionAccounts: "ON",
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
	{
		requireFeatures: features.FeatureSet{
			features.FeatureAccountMetadataHistory: "SYNC",
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
		requireFeatures: features.FeatureSet{
			features.FeatureAccountMetadataHistory: "SYNC",
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
		requireFeatures: features.FeatureSet{
			features.FeatureIndexAddressSegments: "ON",
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
		requireFeatures: features.FeatureSet{
			features.FeatureMovesHistoryPostCommitEffectiveVolumes: "SYNC",
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
		requireFeatures: features.FeatureSet{
			features.FeatureMovesHistoryPostCommitEffectiveVolumes: "SYNC",
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
		requireFeatures: features.FeatureSet{
			features.FeatureHashLogs: "SYNC",
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
}
