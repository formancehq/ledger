package ledger

import (
	"github.com/numary/ledger/core"
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
	validator      core.Validator
}

func NewResolver(lc fx.Lifecycle, validator core.Validator, options ...ResolverOption) *Resolver {
	options = append(DefaultResolverOptions, options...)
	r := &Resolver{
		ledgers:   make(map[string]*Ledger),
		lifecycle: lc,
		validator: validator,
	}
	for _, opt := range options {
		err := opt.apply(r)
		if err != nil {
			panic(errors.Wrap(err, "applying option on resolver"))
		}
	}
	r.validator.Register()
	return r
}

func (r *Resolver) GetLedger(name string) (*Ledger, error) {
	if _, ok := r.ledgers[name]; !ok {
		l, err := NewLedger(name, r.lifecycle, r.storageFactory)

		if err != nil {
			return nil, err
		}

		r.ledgers[name] = l
		r.ledgers[name].validator = r.validator
	}

	return r.ledgers[name], nil
}
