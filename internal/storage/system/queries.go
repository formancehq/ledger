package system

import (
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"
)

type ListLedgersQueryPayload struct {
	Bucket   string
	Features map[string]string
}

type ListLedgersQuery bunpaginate.OffsetPaginatedQuery[ledgerstore.PaginatedQueryOptions[ListLedgersQueryPayload]]

func (q ListLedgersQuery) WithBucket(bucket string) ListLedgersQuery {
	q.Options.Options.Bucket = bucket

	return q
}

func NewListLedgersQuery(pageSize uint64) ListLedgersQuery {
	return ListLedgersQuery{
		PageSize: pageSize,
	}
}
