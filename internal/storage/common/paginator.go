package common

import (
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/uptrace/bun"
)

type Paginator[ResourceType any, PaginationOptions any] interface {
	Paginate(selectQuery *bun.SelectQuery, opts PaginationOptions) (*bun.SelectQuery, error)
	BuildCursor(ret []ResourceType, opts PaginationOptions) (*bunpaginate.Cursor[ResourceType], error)
}
