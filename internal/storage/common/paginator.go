package common

import (
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/uptrace/bun"
)

type Paginator[ResourceType any] interface {
	Paginate(selectQuery *bun.SelectQuery) (*bun.SelectQuery, error)
	BuildCursor(ret []ResourceType) (*bunpaginate.Cursor[ResourceType], error)
}
