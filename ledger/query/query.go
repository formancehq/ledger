package query

const (
	DEFAULT_LIMIT = 15
)

type Query struct {
	Limit  int
	After  string
	Params map[string]interface{}
}

type QueryModifier func(*Query)

func New(qms ...[]QueryModifier) Query {
	q := Query{
		Limit:  DEFAULT_LIMIT,
		Params: map[string]interface{}{},
	}

	for _, m := range qms {
		q.Apply(m)
	}

	return q
}

func (q *Query) Apply(modifiers []QueryModifier) {
	for _, m := range modifiers {
		m(q)
	}
}

func (q *Query) Modify(modifier QueryModifier) {
	modifier(q)
}

func (q *Query) HasParam(name string) bool {
	v, ok := q.Params[name]

	if v == "" {
		ok = false
	}

	return ok
}

func Limit(n int) func(*Query) {
	return func(q *Query) {
		q.Limit = n
	}
}

func After(v string) func(*Query) {
	return func(q *Query) {
		q.After = v
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

func Metakey(v string) func(*Query) {
	return func(q *Query) {
		q.Params["meta_key"] = v
	}
}

func Metavalue(v string) func(*Query) {
	return func(q *Query) {
		q.Params["meta_value"] = v
	}
}
