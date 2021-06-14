package query

const (
	DEFAULT_LIMIT = 25
)

type Query struct {
	Limit int
}

type QueryModifier func(*Query)

func New(qms ...[]QueryModifier) Query {
	q := Query{
		Limit: DEFAULT_LIMIT,
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

func Limit(n int) func(*Query) {
	return func(q *Query) {
		q.Limit = n
	}
}
