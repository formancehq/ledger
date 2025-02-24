package system

import (
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/ledger/internal/storage/ledger"
)

type ListLedgersQuery bunpaginate.OffsetPaginatedQuery[ledger.PaginatedQueryOptions[struct{}]]

func NewListLedgersQuery(pageSize uint64) ListLedgersQuery {
	return ListLedgersQuery{
		PageSize: pageSize,
	}
}
