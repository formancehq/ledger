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

func (store *Store) GetPrefixedRelationName(v string) string {
	return fmt.Sprintf(`"%s".%s`, store.bucket, v)
}

func (store *Store) Name() string {
	return store.name
}

func (store *Store) GetDB() bun.IDB {
	return store.db
}

func (store Store) WithDB(db bun.IDB) *Store {
	store.db = db
	return &store
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
