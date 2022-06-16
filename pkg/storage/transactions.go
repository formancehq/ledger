package storage

import (
	"time"
)

type TransactionsQuery struct {
	Limit     uint
	AfterTxID uint64
	Params    map[string]interface{}
}

type TxQueryModifier func(*TransactionsQuery)

func NewTransactionsQuery(qms ...[]TxQueryModifier) TransactionsQuery {
	q := TransactionsQuery{
		Limit:  QueryDefaultLimit,
		Params: map[string]interface{}{},
	}

	for _, m := range qms {
		q.Apply(m)
	}

	return q
}

func (q *TransactionsQuery) Apply(modifiers []TxQueryModifier) {
	for _, m := range modifiers {
		m(q)
	}
}

func SetAfterTxID(v uint64) func(*TransactionsQuery) {
	return func(q *TransactionsQuery) {
		q.AfterTxID = v
	}
}

func SetStartTime(v time.Time) func(*TransactionsQuery) {
	return func(q *TransactionsQuery) {
		if !v.IsZero() {
			q.Params["start_time"] = v
		}
	}
}

func SetEndTime(v time.Time) func(*TransactionsQuery) {
	return func(q *TransactionsQuery) {
		if !v.IsZero() {
			q.Params["end_time"] = v
		}
	}
}

func SetAccountFilter(v string) func(*TransactionsQuery) {
	return func(q *TransactionsQuery) {
		if v != "" {
			q.Params["account"] = v
		}
	}
}

func SetSourceFilter(v string) func(*TransactionsQuery) {
	return func(q *TransactionsQuery) {
		if v != "" {
			q.Params["source"] = v
		}
	}
}

func SetDestinationFilter(v string) func(*TransactionsQuery) {
	return func(q *TransactionsQuery) {
		if v != "" {
			q.Params["destination"] = v
		}
	}
}

func SetReferenceFilter(v string) func(*TransactionsQuery) {
	return func(q *TransactionsQuery) {
		if v != "" {
			q.Params["reference"] = v
		}
	}
}
