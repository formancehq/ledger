package cache

import (
	"context"
	"strings"

	"github.com/bluele/gcache"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
)

type Cache struct {
	cache gcache.Cache
	store storage.LedgerStore
}

func (c *Cache) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {

	address = strings.TrimPrefix(address, "@")

	rawAccount, err := c.cache.Get(address)
	if err != nil {
		// TODO: Rename later ?
		account, err := c.store.ComputeAccount(ctx, address)
		if err != nil {
			return nil, err
		}

		if err := c.cache.Set(account.Address, account); err != nil {
			panic(err)
		}

		*account = account.Copy()
		return account, nil
	}
	cp := rawAccount.(*core.AccountWithVolumes).Copy()

	return &cp, nil
}

func (c *Cache) Update(accounts core.AccountsAssetsVolumes) {
	for address, volumes := range accounts {
		rawAccount, err := c.cache.Get(address)
		if err != nil {
			// Cannot update cache, item maybe evicted
			continue
		}
		account := rawAccount.(*core.AccountWithVolumes)
		account.Volumes = volumes
		account.Balances = volumes.Balances()
		if err := c.cache.Set(address, account); err != nil {
			panic(err)
		}
	}
}

func (c *Cache) UpdateAccountMetadata(ctx context.Context, address string, m core.Metadata) error {
	account, err := c.GetAccountWithVolumes(ctx, address)
	if err != nil {
		return err
	}
	account.Metadata = account.Metadata.Merge(m)
	_ = c.cache.Set(address, account)
	return nil
}

func New(store storage.LedgerStore) *Cache {
	return &Cache{
		store: store,
		//TODO(gfyrag): Make configurable
		cache: gcache.New(1000).LFU().Build(),
	}
}
