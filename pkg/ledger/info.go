package ledger

import (
	"context"
	"time"

	"github.com/numary/ledger/pkg/core"
	"github.com/pkg/errors"
)

func (l *Ledger) GetMigrationsInfo(ctx context.Context) ([]core.MigrationInfo, error) {
	migrationsAvailable, err := l.store.GetMigrationsAvailable()
	if err != nil {
		return []core.MigrationInfo{}, errors.Wrap(err, "getting migrations available")
	}

	migrationsDone, err := l.store.GetMigrationsDone(ctx)
	if err != nil {
		return []core.MigrationInfo{}, errors.Wrap(err, "getting migrations done")
	}

	res := make([]core.MigrationInfo, 0)
	for _, mAvailable := range migrationsAvailable {
		timestamp := time.Time{}
		done := false
		for _, mDone := range migrationsDone {
			if mDone.Version == mAvailable.Version {
				done = true
				timestamp = mDone.Date
				break
			}
		}
		if done {
			res = append(res, core.MigrationInfo{
				Version: mAvailable.Version,
				Name:    mAvailable.Name,
				Date:    timestamp,
				State:   "DONE",
			})
		} else {
			res = append(res, core.MigrationInfo{
				Version: mAvailable.Version,
				Name:    mAvailable.Name,
				State:   "TO DO",
			})
		}
	}

	return res, nil
}
