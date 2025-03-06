package system

import (
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/ledger/internal/storage/common"
)

type ListLedgersQueryPayload struct {
	Bucket   string
	Features map[string]string
}

func NewListLedgersQuery(pageSize uint64) common.ColumnPaginatedQuery[ListLedgersQueryPayload] {
	return common.ColumnPaginatedQuery[ListLedgersQueryPayload]{
		PageSize: pageSize,
		Column:   "id",
		Order:    (*bunpaginate.Order)(pointer.For(bunpaginate.OrderAsc)),
		Options: common.ResourceQuery[ListLedgersQueryPayload]{
			Expand: make([]string, 0),
		},
	}
}
