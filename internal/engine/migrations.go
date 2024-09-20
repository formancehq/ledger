package engine

import (
	"context"

	"github.com/formancehq/go-libs/migrations"
)

func (l *Ledger) GetMigrationsInfo(ctx context.Context) ([]migrations.Info, error) {
	return l.store.GetMigrationsInfo(ctx)
}
