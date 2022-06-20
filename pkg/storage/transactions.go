package storage

import (
	"time"
)

// please use the builder NewTransactionsQuery() if you're not sure what you're doing
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

func NewTransactionsQuery(limit uint, afterTxID uint64, filters *TransactionsQueryFilters) TransactionsQuery {
	q := TransactionsQuery{
		Limit: QueryDefaultLimit,
	}

	if limit != 0 {
		q.Limit = limit
	}

	q.AfterTxID = afterTxID

	if filters != nil {

		if !filters.StartTime.IsZero() {
			q.Params.StartTime = filters.StartTime
		}
		if !filters.EndTime.IsZero() {
			q.Params.EndTime = filters.EndTime
		}

		q.Params.Account = filters.Account
		q.Params.Destination = filters.Destination
		q.Params.Reference = filters.Reference
		q.Params.Source = filters.Source
	}

	return q
}
