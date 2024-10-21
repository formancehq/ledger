package legacy

import (
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
)

type Store struct {
	db bun.IDB

	bucket string
	name   string
}

func (s *Store) GetPrefixedRelationName(v string) string {
	return fmt.Sprintf(`"%s".%s`, s.bucket, v)
}

func (store *Store) Name() string {
	return store.name
}

func (store *Store) GetDB() bun.IDB {
	return store.db
}

func (s *Store) WithDB(db bun.IDB) *Store {
	ret := *s
	ret.db = db
	return &ret
}

func New(
	db bun.IDB,
	bucket string,
	name string,
) *Store {
	return &Store{
		db:     db,
		bucket: bucket,
		name:   name,
	}
}
