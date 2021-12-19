package ledger

import (
	"github.com/numary/ledger/storage"
	"github.com/pkg/errors"
	"go.uber.org/fx"
)

type ResolverOption interface {
	apply(r *Resolver) error
}
type ResolveOptionFn func(r *Resolver) error

func (fn ResolveOptionFn) apply(r *Resolver) error {
	return fn(r)
}

func WithStorageFactory(factory storage.Factory) ResolveOptionFn {
	return ResolveOptionFn(func(r *Resolver) error {
		r.storageFactory = factory
		return nil
	})
}

var DefaultResolverOptions = []ResolverOption{
	WithStorageFactory(storage.DefaultFactory),
}

type Resolver struct {
	lifecycle      fx.Lifecycle
	ledgers        map[string]*Ledger
	storageFactory storage.Factory
}

func NewResolver(lc fx.Lifecycle, options ...ResolverOption) *Resolver {
	options = append(DefaultResolverOptions, options...)
	r := &Resolver{
		ledgers:   make(map[string]*Ledger),
		lifecycle: lc,
	}
	for _, opt := range options {
		err := opt.apply(r)
		if err != nil {
			panic(errors.Wrap(err, "applying option on resolver"))
		}
	}

	return r
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
