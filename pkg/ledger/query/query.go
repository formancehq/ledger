package query

import (
	"time"
)

const (
	DefaultLimit = 15
)

type Query struct {
	Limit  int
	After  string
	Params map[string]interface{}
}

type Modifier func(*Query)

func New(qms ...[]Modifier) Query {
	q := Query{
		Limit:  DefaultLimit,
		Params: map[string]interface{}{},
	}

	for _, m := range qms {
		q.Apply(m)
	}

	return q
}

func (q *Query) Apply(modifiers []Modifier) {
	for _, m := range modifiers {
		m(q)
	}
}

func (q *Query) Modify(modifier Modifier) {
	modifier(q)
}

func (q *Query) HasParam(name string) bool {
	v, ok := q.Params[name]

	if v == "" {
		ok = false
	}

	return ok
}

func After(v string) func(*Query) {
	return func(q *Query) {
		q.After = v
	}
}

func StartTime(v time.Time) func(*Query) {
	return func(q *Query) {
		q.Params["start_time"] = v
	}
}

func EndTime(v time.Time) func(*Query) {
	return func(q *Query) {
		q.Params["end_time"] = v
	}
}

func Address(v string) func(*Query) {
	return func(q *Query) {
		q.Params["address"] = v
	}
}

func Account(v string) func(*Query) {
	return func(q *Query) {
		q.Params["account"] = v
	}
}

func Source(v string) func(*Query) {
	return func(q *Query) {
		q.Params["source"] = v
	}
}

func Destination(v string) func(*Query) {
	return func(q *Query) {
		q.Params["destination"] = v
	}
}

func Reference(v string) func(*Query) {
	return func(q *Query) {
		q.Params["reference"] = v
	}
}

func Metadata(v map[string]string) func(*Query) {
	return func(q *Query) {
		q.Params["metadata"] = v
	}
}
