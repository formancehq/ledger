package vm

import (
	"context"
	"math/big"

	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger/internal"
)

// BalanceQuery is a map of account/asset
type BalanceQuery map[string][]string

// Balances is a map of account/asset/balance
type Balances map[string]map[string]*big.Int

type Store interface {
	GetBalances(ctx context.Context, query BalanceQuery) (Balances, error)
	GetAccount(ctx context.Context, address string) (*ledger.Account, error)
}

type emptyStore struct{}

func (e *emptyStore) GetBalances(context.Context, BalanceQuery) (Balances, error) {
	return Balances{}, nil
}

func (e *emptyStore) GetAccount(_ context.Context, address string) (*ledger.Account, error) {
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

func (s StaticStore) GetBalances(_ context.Context, query BalanceQuery) (Balances, error) {
	ret := Balances{}
	for accountAddress, assets := range query {
		for _, asset := range assets {
			ret[accountAddress] = make(map[string]*big.Int)
			account, ok := s[accountAddress]
			if !ok {
				ret[accountAddress] = map[string]*big.Int{
					asset: new(big.Int),
				}
				continue
			}
			balance, ok := account.Balances[asset]
			if !ok {
				ret[accountAddress][asset] = new(big.Int)
				continue
			}
			ret[accountAddress][asset] = balance
		}
	}

	return ret, nil
}

func (s StaticStore) GetAccount(_ context.Context, address string) (*ledger.Account, error) {
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
