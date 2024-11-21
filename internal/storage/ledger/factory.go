package ledger

import (
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/bucket"
	"github.com/uptrace/bun"
)

type Factory interface {
	Create(bucket.Bucket, ledger.Ledger) *Store
}

type DefaultFactory struct {
	db      *bun.DB
	options []Option
}

func NewFactory(db *bun.DB, options ...Option) *DefaultFactory {
	return &DefaultFactory{
		db:      db,
		options: options,
	}
}

func (d *DefaultFactory) Create(b bucket.Bucket, l ledger.Ledger) *Store {
	return New(d.db, b, l, d.options...)
}
