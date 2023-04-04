package storage

import (
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type TransactionsQuery ColumnPaginatedQuery[TransactionsQueryFilters]

func NewTransactionsQuery() TransactionsQuery {
	return TransactionsQuery{
		PageSize: QueryDefaultPageSize,
		Column:   "id",
		Order:    OrderDesc,
		Filters: TransactionsQueryFilters{
			Metadata: metadata.Metadata{},
		},
	}
}

type TransactionsQueryFilters struct {
	AfterTxID   uint64            `json:"afterTxID,omitempty"`
	Reference   string            `json:"reference,omitempty"`
	Destination string            `json:"destination,omitempty"`
	Source      string            `json:"source,omitempty"`
	Account     string            `json:"account,omitempty"`
	EndTime     core.Time         `json:"endTime,omitempty"`
	StartTime   core.Time         `json:"startTime,omitempty"`
	Metadata    metadata.Metadata `json:"metadata,omitempty"`
}

func (a TransactionsQuery) WithPageSize(pageSize uint64) TransactionsQuery {
	if pageSize != 0 {
		a.PageSize = pageSize
	}

	return a
}

func (a TransactionsQuery) WithAfterTxID(after uint64) TransactionsQuery {
	a.Filters.AfterTxID = after

	return a
}

func (a TransactionsQuery) WithStartTimeFilter(start core.Time) TransactionsQuery {
	if !start.IsZero() {
		a.Filters.StartTime = start
	}

	return a
}

func (a TransactionsQuery) WithEndTimeFilter(end core.Time) TransactionsQuery {
	if !end.IsZero() {
		a.Filters.EndTime = end
	}

	return a
}

func (a TransactionsQuery) WithAccountFilter(account string) TransactionsQuery {
	a.Filters.Account = account

	return a
}

func (a TransactionsQuery) WithDestinationFilter(dest string) TransactionsQuery {
	a.Filters.Destination = dest

	return a
}

func (a TransactionsQuery) WithReferenceFilter(ref string) TransactionsQuery {
	a.Filters.Reference = ref

	return a
}

func (a TransactionsQuery) WithSourceFilter(source string) TransactionsQuery {
	a.Filters.Source = source

	return a
}

func (a TransactionsQuery) WithMetadataFilter(metadata metadata.Metadata) TransactionsQuery {
	a.Filters.Metadata = metadata

	return a
}
