package ledger

import (
	"context"
	"embed"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/migrations"
	"github.com/pkg/errors"
)

func (s *Store) GetMigrationsDone(ctx context.Context) ([]core.MigrationInfo, error) {
	migrations, err := migrations.GetMigrations(ctx, s.schema)
	return migrations, s.error(err)
}

func (s *Store) GetMigrationsAvailable() ([]core.MigrationInfo, error) {
	migrations, err := migrations.CollectMigrationFiles(MigrationsFS)
	if err != nil {
		return []core.MigrationInfo{}, errors.Wrap(err, "collecting migration files")
	}

	res := make([]core.MigrationInfo, 0)
	for _, m := range migrations {
		res = append(res, core.MigrationInfo{
			Version: m.Version,
			Name:    m.Name,
		})
	}

	return res, nil
}

//go:embed migrates
var MigrationsFS embed.FS
