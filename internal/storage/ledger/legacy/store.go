package ledgerstore

import (
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/uptrace/bun"
)

type Store struct {
	db *bun.DB

	bucket string
	name   string
}

func (s *Store) GetPrefixedRelationName(v string) string {
	return fmt.Sprintf(`"%s".%s`, s.bucket, v)
}

func (store *Store) Name() string {
	return store.name
}

func (store *Store) GetDB() *bun.DB {
	return store.db
}

func New(
	db *bun.DB,
	bucket string,
	name string,
) *Store {
	return &Store{
		db:     db,
		bucket: bucket,
		name:   name,
	}
}
