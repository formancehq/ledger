package systemstore

import (
	"context"

	"github.com/formancehq/ledger/internal/storage"
	"github.com/formancehq/stack/libs/go-libs/migrations"
	"github.com/uptrace/bun"
)

func (s *Store) getMigrator() *migrations.Migrator {
	migrator := migrations.NewMigrator(migrations.WithSchema("_system", true))
	migrator.RegisterMigrations(
		migrations.Migration{
			Name: "Init schema",
			UpWithContext: func(ctx context.Context, tx bun.Tx) error {
				_, err := tx.NewCreateTable().
					Model((*Ledgers)(nil)).
					IfNotExists().
					Exec(ctx)
				if err != nil {
					return storage.PostgresError(err)
				}

				_, err = s.db.NewCreateTable().
					Model((*configuration)(nil)).
					IfNotExists().
					Exec(ctx)
				return storage.PostgresError(err)
			},
		},
	)
	return migrator
}
