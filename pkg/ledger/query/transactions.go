package query

import (
	"time"
)

const (
	DefaultLimit = 15
)

type Transactions struct {
	Limit     uint
	AfterTxID uint64
	Params    map[string]interface{}
}

type TxModifier func(*Transactions)

func NewTransactions(qms ...[]TxModifier) Transactions {
	q := Transactions{
		Limit:  DefaultLimit,
		Params: map[string]interface{}{},
	}

	for _, m := range qms {
		q.Apply(m)
	}

	return q
}

func (q *Transactions) Apply(modifiers []TxModifier) {
	for _, m := range modifiers {
		m(q)
	}
}

func SetAfterTxID(v uint64) func(*Transactions) {
	return func(q *Transactions) {
		q.AfterTxID = v
	}
}

func SetStartTime(v time.Time) func(*Transactions) {
	return func(q *Transactions) {
		if !v.IsZero() {
			q.Params["start_time"] = v
		}
	}
}

func SetEndTime(v time.Time) func(*Transactions) {
	return func(q *Transactions) {
		if !v.IsZero() {
			q.Params["end_time"] = v
		}
	}
}

func SetAccountFilter(v string) func(*Transactions) {
	return func(q *Transactions) {
		if v != "" {
			q.Params["account"] = v
		}
	}
}

func SetSourceFilter(v string) func(*Transactions) {
	return func(q *Transactions) {
		if v != "" {
			q.Params["source"] = v
		}
	}
}

func SetDestinationFilter(v string) func(*Transactions) {
	return func(q *Transactions) {
		if v != "" {
			q.Params["destination"] = v
		}
	}
}

func SetReferenceFilter(v string) func(*Transactions) {
	return func(q *Transactions) {
		if v != "" {
			q.Params["reference"] = v
		}
	}
}
