package query

type Accounts struct {
	Limit        uint
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

func SetAfterAddress(v string) func(*Accounts) {
	return func(q *Accounts) {
		q.AfterAddress = v
	}
}

func SetAddressRegexpFilter(v string) func(*Accounts) {
	return func(q *Accounts) {
		if v != "" {
			q.Params["address"] = v
		}
	}
}

func SetMetadataFilter(v map[string]string) func(*Accounts) {
	return func(q *Accounts) {
		if len(v) > 0 {
			q.Params["metadata"] = v
		}
	}
}
