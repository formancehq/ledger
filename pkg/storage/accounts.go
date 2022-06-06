package storage

type AccountsQuery struct {
	Limit        uint
	Offset       uint
	AfterAddress string
	Params       map[string]interface{}
}

type AccModifier func(*AccountsQuery)

type BalanceOperator string

const (
	BalanceOperatorE   BalanceOperator = "e"
	BalanceOperatorGt  BalanceOperator = "gt"
	BalanceOperatorGte BalanceOperator = "gte"
	BalanceOperatorLt  BalanceOperator = "lt"
	BalanceOperatorLte BalanceOperator = "lte"
)

func (b BalanceOperator) IsValid() bool {
	switch b {
	case BalanceOperatorE,
		BalanceOperatorGt,
		BalanceOperatorGte,
		BalanceOperatorLt,
		BalanceOperatorLte:
		return true
	}

	return false
}

func NewBalanceOperator(s string) (BalanceOperator, bool) {
	if !BalanceOperator(s).IsValid() {
		return "", false
	}

	return BalanceOperator(s), true
}

func SetBalanceFilter(v string) func(*AccountsQuery) {
	return func(q *AccountsQuery) {
		q.Params["balance"] = v
	}
}

func SetBalanceOperatorFilter(v BalanceOperator) func(*AccountsQuery) {
	return func(q *AccountsQuery) {
		q.Params["balance_operator"] = v
	}
}

func NewAccountsQuery(qms ...[]AccModifier) AccountsQuery {
	q := AccountsQuery{
		Limit:  QueryDefaultLimit,
		Params: map[string]interface{}{},
	}

	for _, m := range qms {
		q.Apply(m)
	}

	return q
}

func (q *AccountsQuery) Apply(modifiers []AccModifier) {
	for _, m := range modifiers {
		m(q)
	}
}

func SetOffset(v uint) func(accounts *AccountsQuery) {
	return func(q *AccountsQuery) {
		q.Offset = v
	}
}

func SetAfterAddress(v string) func(*AccountsQuery) {
	return func(q *AccountsQuery) {
		q.AfterAddress = v
	}
}

func SetAddressRegexpFilter(v string) func(*AccountsQuery) {
	return func(q *AccountsQuery) {
		if v != "" {
			q.Params["address"] = v
		}
	}
}

func SetMetadataFilter(v map[string]string) func(*AccountsQuery) {
	return func(q *AccountsQuery) {
		if len(v) > 0 {
			q.Params["metadata"] = v
		}
	}
}
