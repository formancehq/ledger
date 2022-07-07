package storage

import (
	"time"
)

type TransactionsQuery struct {
	Limit     uint
	AfterTxID uint64
	Filters   TransactionsQueryFilters
}

type TransactionsQueryFilters struct {
	Reference   string
	Destination string
	Source      string
	Account     string
	EndTime     time.Time
	StartTime   time.Time
	Metadata    map[string]string
}

func NewTransactionsQuery() *TransactionsQuery {

	return &TransactionsQuery{
		Limit: QueryDefaultLimit,
	}
}

func (a *TransactionsQuery) WithLimit(limit uint) *TransactionsQuery {
	if limit != 0 {
		a.Limit = limit
	}

	return a
}

func (a *TransactionsQuery) WithAfterTxID(after uint64) *TransactionsQuery {
	a.AfterTxID = after

	return a
}

func (a *TransactionsQuery) WithStartTimeFilter(start time.Time) *TransactionsQuery {
	if !start.IsZero() {
		a.Filters.StartTime = start
	}

	return a
}

func (a *TransactionsQuery) WithEndTimeFilter(end time.Time) *TransactionsQuery {
	if !end.IsZero() {
		a.Filters.EndTime = end
	}

	return a
}

func (a *TransactionsQuery) WithAccountFilter(account string) *TransactionsQuery {
	a.Filters.Account = account

	return a
}

func (a *TransactionsQuery) WithDestinationFilter(dest string) *TransactionsQuery {
	a.Filters.Destination = dest

	return a
}

func (a *TransactionsQuery) WithReferenceFilter(ref string) *TransactionsQuery {
	a.Filters.Reference = ref

	return a
}

func (a *TransactionsQuery) WithSourceFilter(source string) *TransactionsQuery {
	a.Filters.Source = source

	return a
}

func (a *TransactionsQuery) WithMetadataFilter(metadata map[string]string) *TransactionsQuery {
	a.Filters.Metadata = metadata

	return a
}
