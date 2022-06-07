package query

type Accounts struct {
	Limit        uint64
	Offset       uint64
	AfterAddress string
	Params       map[string]interface{}
}

type AccModifier func(*Accounts)

func NewAccounts(qms ...[]AccModifier) Accounts {
	q := Accounts{
		Limit:  DefaultLimit,
		Params: map[string]interface{}{},
	}

	for _, m := range qms {
		q.Apply(m)
	}

	return q
}

func (q *Accounts) Apply(modifiers []AccModifier) {
	for _, m := range modifiers {
		m(q)
	}
}

func (q *Accounts) Modify(modifier AccModifier) {
	modifier(q)
}

func SetOffset(v uint64) func(accounts *Accounts) {
	return func(q *Accounts) {
		q.Offset = v
	}
}

func SetAfterAddress(v string) func(accounts *Accounts) {
	return func(q *Accounts) {
		q.AfterAddress = v
	}
}

func SetAddressRegexpFilter(v string) func(accounts *Accounts) {
	return func(q *Accounts) {
		if v != "" {
			q.Params["address"] = v
		}
	}
}

func SetMetadataFilter(v map[string]string) func(accounts *Accounts) {
	return func(q *Accounts) {
		if len(v) > 0 {
			q.Params["metadata"] = v
		}
	}
}
