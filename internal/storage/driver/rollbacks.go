package driver

import (
	"context"
	"errors"
	"fmt"
	"github.com/formancehq/go-libs/v2/migrations"
)

func detectDowngrades(migrator *migrations.Migrator, ctx context.Context) error {
	lastVersion, err := migrator.GetLastVersion(ctx)
	if err != nil {
		if !errors.Is(err, migrations.ErrMissingVersionTable) {
			return fmt.Errorf("failed to get last version: %w", err)
		}
	}
	if err == nil && lastVersion != -1 {
		allMigrations, err := migrator.GetMigrations(ctx)
		if err != nil {
			return fmt.Errorf("failed to get all migrations: %w", err)
		}

		if len(allMigrations) < lastVersion {
			return newErrRollbackDetected(lastVersion, len(allMigrations))
		}
	}

	return nil
}
