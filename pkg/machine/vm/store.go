package vm

import (
	"context"
	"database/sql"

	"github.com/formancehq/ledger/pkg/core"
)

type Store interface {
	GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error)
}
type StoreFn func(ctx context.Context, address string) (*core.AccountWithVolumes, error)

func (fn StoreFn) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	return fn(ctx, address)
}

var EmptyStore = StoreFn(func(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	return nil, nil
})

type StaticStore map[string]*core.AccountWithVolumes

func (s StaticStore) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	v, ok := s[address]
	if !ok {
		return nil, sql.ErrNoRows
	}
	return v, nil
}

var _ Store = (*StaticStore)(nil)
