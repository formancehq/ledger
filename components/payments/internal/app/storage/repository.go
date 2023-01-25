package storage

import (
	"github.com/uptrace/bun"
	"github.com/uptrace/bun/extra/bundebug"
)

type Storage struct {
	db *bun.DB
}

func newStorage(db *bun.DB) *Storage {
	return &Storage{db: db}
}

// nolint:unused // used for SQL debugging purposes
func (s *Storage) debug() {
	s.db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(true)))
}
