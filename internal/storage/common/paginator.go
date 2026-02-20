package common

import (
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
)

type Paginator[ResourceType any] interface {
	Paginate(selectQuery *bun.SelectQuery) (*bun.SelectQuery, error)
	BuildCursor(ret []ResourceType) (*bunpaginate.Cursor[ResourceType], error)
}
