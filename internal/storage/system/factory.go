package system

import (
	"github.com/uptrace/bun"
)

type StoreFactory interface {
	Create(db bun.IDB) Store
}

type DefaultStoreFactory struct {
	options []Option
}

func (s DefaultStoreFactory) Create(db bun.IDB) Store {
	return New(db, s.options...)
}

var _ StoreFactory = DefaultStoreFactory{}

func NewStoreFactory(opts ...Option) DefaultStoreFactory {
	return DefaultStoreFactory{options: opts}
}