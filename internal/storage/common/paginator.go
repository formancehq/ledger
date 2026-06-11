package common

import (
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
)

type Paginator[ResourceType any] interface {
	Paginate(selectQuery *bun.SelectQuery) (*bun.SelectQuery, error)
	BuildCursor(ret []ResourceType) (*paginate.Cursor[ResourceType], error)
	// OrderExpression returns the ORDER BY expression used by this paginator,
	// so the outer CTE wrapper can re-apply it without a row_number() window function.
	OrderExpression() string
	// ApplyCursorPredicate adds only the page-position predicate that belongs to "the filtered
	// set" — for column pagination the keyset WHERE (id <=/>=/... cursor); for offset pagination
	// nothing (the offset is applied as part of the page window). Used by the MATERIALIZED-CTE
	// fence path, where this predicate must stay inside the fenced dataset CTE.
	ApplyCursorPredicate(selectQuery *bun.SelectQuery) *bun.SelectQuery
	// ApplyWindow adds only LIMIT (and OFFSET for offset pagination) — it does NOT add ORDER BY.
	// The caller must have already applied the paginator's order (see OrderExpression) to the
	// select. Used by the fence path to attach the page window to the outer select.
	ApplyWindow(selectQuery *bun.SelectQuery) (*bun.SelectQuery, error)
}
