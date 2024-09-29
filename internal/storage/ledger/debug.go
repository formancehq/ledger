package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/shomali11/xsql"
	"github.com/uptrace/bun"
)

// nolint:unused
func (s *Store) dumpTables(ctx context.Context, tables ...string) {
	for _, table := range tables {
		s.dumpQuery(
			ctx,
			s.db.NewSelect().
				ModelTableExpr(s.GetPrefixedRelationName(table)),
		)
	}
}

// nolint:unused
func (s *Store) dumpQuery(ctx context.Context, query *bun.SelectQuery) {
	fmt.Println(query)
	rows, err := query.Rows(ctx)
	if err != nil {
		panic(err)
	}
	s.dumpRows(rows)
}

// nolint:unused
func (s *Store) dumpRows(rows *sql.Rows) {
	data, err := xsql.Pretty(rows)
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
	if err := rows.Close(); err != nil {
		panic(err)
	}
}
