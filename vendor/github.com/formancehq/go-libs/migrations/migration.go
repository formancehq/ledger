package migrations

import (
	"context"

	"github.com/uptrace/bun"
)

type Migration struct {
	Name          string
	Up            func(tx bun.Tx) error
	UpWithContext func(ctx context.Context, tx bun.Tx) error
}
