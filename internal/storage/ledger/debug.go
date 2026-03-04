package ledger

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/shomali11/xsql"
	"github.com/uptrace/bun"
)

//nolint:unused
func (store *Store) DumpTables(ctx context.Context, tables ...string) {
	for _, table := range tables {
		store.DumpQuery(
			ctx,
			store.db.NewSelect().
				ModelTableExpr(store.GetPrefixedRelationName(table)),
		)
	}
}

//nolint:unused
func (store *Store) DumpQuery(ctx context.Context, query *bun.SelectQuery) {
	fmt.Println(query)
	rows, err := query.Rows(ctx)
	if err != nil {
		panic(err)
	}
	store.DumpRows(rows)
}

//nolint:unused
func (store *Store) DumpRows(rows *sql.Rows) {
	data, err := xsql.Pretty(rows)
	if err != nil {
		panic(err)
	}
	fmt.Println(data)
	if err := rows.Close(); err != nil {
		panic(err)
	}
}
