package vm

import (
	"context"
	"math/big"

	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/v2/internal"
)

type Store interface {
	GetBalance(ctx context.Context, address, asset string) (*big.Int, error)
	GetAccount(ctx context.Context, address string) (*ledger.Account, error)
}

type emptyStore struct{}

func (e *emptyStore) GetBalance(ctx context.Context, address, asset string) (*big.Int, error) {
	return new(big.Int), nil
}

func (e *emptyStore) GetAccount(ctx context.Context, address string) (*ledger.Account, error) {
	return &ledger.Account{
		Address:  address,
		Metadata: metadata.Metadata{},
	}, nil
}

var _ Store = (*emptyStore)(nil)

var EmptyStore = &emptyStore{}

type AccountWithBalances struct {
	ledger.Account
	Balances map[string]*big.Int
}

type StaticStore map[string]*AccountWithBalances

func (s StaticStore) GetBalance(ctx context.Context, address, asset string) (*big.Int, error) {
	account, ok := s[address]
	if !ok {
		return new(big.Int), nil
	}
	balance, ok := account.Balances[asset]
	if !ok {
		return new(big.Int), nil
	}

	return balance, nil
}

func (s StaticStore) GetAccount(ctx context.Context, address string) (*ledger.Account, error) {
	account, ok := s[address]
	if !ok {
		return &ledger.Account{
			Address:  address,
			Metadata: metadata.Metadata{},
		}, nil
	}

	return &account.Account, nil
}

var _ Store = StaticStore{}
