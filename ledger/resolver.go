package ledger

import (
	"github.com/numary/ledger/storage"
	"go.uber.org/fx"
)

type Resolver struct {
	lifecycle      fx.Lifecycle
	ledgers        map[string]*Ledger
	storageFactory storage.Factory
}

func NewResolver(lc fx.Lifecycle, storageFactory storage.Factory) *Resolver {
	return &Resolver{
		ledgers:        make(map[string]*Ledger),
		lifecycle:      lc,
		storageFactory: storageFactory,
	}
}

func (r *Resolver) GetLedger(name string) (*Ledger, error) {
	if _, ok := r.ledgers[name]; !ok {
		l, err := NewLedger(name, r.lifecycle, r.storageFactory)

		if err != nil {
			return nil, err
		}

		r.ledgers[name] = l
	}

	return r.ledgers[name], nil
}
