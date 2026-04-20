package common

import (
	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
)

type Paginator[ResourceType any] interface {
	Paginate(selectQuery *bun.SelectQuery) (*bun.SelectQuery, error)
	BuildCursor(ret []ResourceType) (*paginate.Cursor[ResourceType], error)
}
