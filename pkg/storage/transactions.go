package storage

import (
	"time"
)

type TransactionsQuery struct {
	Limit     uint
	AfterTxID uint64
	Params    TransactionsQueryFilters
}

type TransactionsQueryFilters struct {
	Reference   string
	Destination string
	Source      string
	Account     string
	EndTime     time.Time
	StartTime   time.Time
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
		a.Params.StartTime = start
	}

	return a
}

func (a *TransactionsQuery) WithEndTimeFilter(end time.Time) *TransactionsQuery {
	if !end.IsZero() {
		a.Params.EndTime = end
	}

	return a
}

func (a *TransactionsQuery) WithAccountFilter(account string) *TransactionsQuery {
	a.Params.Account = account

	return a
}

func (a *TransactionsQuery) WithDestinationFilter(dest string) *TransactionsQuery {
	a.Params.Destination = dest

	return a
}

func (a *TransactionsQuery) WithReferenceFilter(ref string) *TransactionsQuery {
	a.Params.Reference = ref

	return a
}

func (a *TransactionsQuery) WithSourceFilter(source string) *TransactionsQuery {
	a.Params.Source = source

	return a
}
