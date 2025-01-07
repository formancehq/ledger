package storage

import (
	"context"

	"github.com/formancehq/go-libs/v2/migrations"
	"github.com/uptrace/bun"
)

func NewMigrator(db *bun.DB) *migrations.Migrator {
	migrator := migrations.NewMigrator(db)
	migrator.RegisterMigrations(
		migrations.Migration{
			Name: "init-database",
			Up: func(ctx context.Context, db bun.IDB) error {
				_, err := db.ExecContext(ctx, `
					create table connectors (
					    id varchar,
					    driver varchar,
					    config varchar,
					    created_at timestamp,
					    
					    primary key(id)   
					);

					create table pipelines (
					    id varchar,
					    module varchar,
					    connector_id varchar references connectors (id),
					    created_at timestamp,
					    state jsonb,
					    disabled bool,
					    
					    primary key(id)
					);
					create unique index on pipelines (module, connector_id);
				`)
				return err
			},
		},
	)
	return migrator
}
