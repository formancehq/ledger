package ledger

import (
	"go.uber.org/fx"
)

type Resolver struct {
	lifecycle fx.Lifecycle
	ledgers   map[string]*Ledger
}

func NewResolver(lc fx.Lifecycle) *Resolver {
	return &Resolver{
		ledgers:   make(map[string]*Ledger),
		lifecycle: lc,
	}
}

func (r *Resolver) GetLedger(name string) (*Ledger, error) {
	if _, ok := r.ledgers[name]; !ok {
		l, err := NewLedger(name, r.lifecycle)

		if err != nil {
			return nil, err
		}

		r.ledgers[name] = l
	}

	return r.ledgers[name], nil
}
