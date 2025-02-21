package ledger

import (
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/uptrace/bun"
)

type paginator[ResourceType any, PaginationOptions any] interface {
	paginate(selectQuery *bun.SelectQuery, opts PaginationOptions) (*bun.SelectQuery, error)
	buildCursor(ret []ResourceType, opts PaginationOptions) (*bunpaginate.Cursor[ResourceType], error)
}
