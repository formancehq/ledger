package legacy

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/shomali11/xsql"
	"github.com/uptrace/bun"
)

//nolint:unused
func (s *Store) DumpTables(ctx context.Context, tables ...string) {
	for _, table := range tables {
		s.DumpQuery(
			ctx,
			s.db.NewSelect().
				ModelTableExpr(s.GetPrefixedRelationName(table)),
		)
	}
}

//nolint:unused
func (s *Store) DumpQuery(ctx context.Context, query *bun.SelectQuery) {
	fmt.Println(query)
	rows, err := query.Rows(ctx)
	if err != nil {
		panic(err)
	}
	s.DumpRows(rows)
}

//nolint:unused
func (s *Store) DumpRows(rows *sql.Rows) {
	data, err := xsql.Pretty(rows)
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
	if err := rows.Close(); err != nil {
		panic(err)
	}
}
