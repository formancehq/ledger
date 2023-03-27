package vm

import (
	"context"

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
	return &core.AccountWithVolumes{
		Account: core.Account{
			Address:  address,
			Metadata: core.Metadata{},
		},
		Volumes: map[string]core.Volumes{},
	}, nil
})

type StaticStore map[string]*core.AccountWithVolumes

func (s StaticStore) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	v, ok := s[address]
	if !ok {
		return &core.AccountWithVolumes{
			Account: core.Account{
				Address:  address,
				Metadata: core.Metadata{},
			},
			Volumes: map[string]core.Volumes{},
		}, nil
	}
	return v, nil
}

var _ Store = StaticStore{}
