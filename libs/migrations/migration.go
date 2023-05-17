package migrations

import (
	"github.com/uptrace/bun"
)

type Migration struct {
	Up func(tx bun.Tx) error
}
