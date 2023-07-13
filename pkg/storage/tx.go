package storage

import (
	"github.com/uptrace/bun"
)

type Tx struct {
	schema *Schema
	bun.Tx
}

func (s *Tx) NewSelect(tableName string) *bun.SelectQuery {
	return s.Tx.NewSelect().ModelTableExpr("?0.?1 as ?1", bun.Ident(s.schema.Name()), bun.Ident(tableName))
}

func (s *Tx) NewInsert(tableName string) *bun.InsertQuery {
	return s.Tx.NewInsert().ModelTableExpr("?0.?1 as ?1", bun.Ident(s.schema.Name()), bun.Ident(tableName))
}

func (s *Tx) NewUpdate(tableName string) *bun.UpdateQuery {
	return s.Tx.NewUpdate().ModelTableExpr("?0.?1 as ?1", bun.Ident(s.schema.Name()), bun.Ident(tableName))
}
