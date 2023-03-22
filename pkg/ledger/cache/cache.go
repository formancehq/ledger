package cache

import (
	"context"
	"strings"
	"sync"

	"github.com/bluele/gcache"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage"
)

type cacheEntry struct {
	sync.Mutex
	account *core.AccountWithVolumes
}

type Cache struct {
	cache gcache.Cache
	store storage.LedgerStore
}

func (c *Cache) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {

	address = strings.TrimPrefix(address, "@")

	entry, err := c.cache.Get(address)
	if err != nil {
		// TODO: Rename later ?
		account, err := c.store.ComputeAccount(ctx, address)
		if err != nil {
			return nil, err
		}

		ce := &cacheEntry{
			account: account,
		}

		if err := c.cache.Set(account.Address, ce); err != nil {
			panic(err)
		}

		*account = account.Copy()
		return account, nil
	}
	cp := entry.(*cacheEntry).account.Copy()

	return &cp, nil
}

func (c *Cache) withLockOnAccount(address string, callback func(account *core.AccountWithVolumes)) {
	item, err := c.cache.Get(address)
	if err != nil {
		return
	}
	entry := item.(*cacheEntry)
	entry.Lock()
	defer entry.Unlock()

	callback(entry.account)
}

func (c *Cache) addOutput(address, asset string, amount *core.MonetaryInt) {
	c.withLockOnAccount(address, func(account *core.AccountWithVolumes) {
		volumes := account.Volumes[asset]
		volumes.Output = volumes.Output.OrZero().Add(amount)
		volumes.Input = volumes.Input.OrZero()
		account.Volumes[asset] = volumes
	})
}

func (c *Cache) addInput(address, asset string, amount *core.MonetaryInt) {
	c.withLockOnAccount(address, func(account *core.AccountWithVolumes) {
		volumes := account.Volumes[asset]
		volumes.Input = volumes.Input.OrZero().Add(amount)
		volumes.Output = volumes.Output.OrZero()
		account.Volumes[asset] = volumes
	})
}

func (c *Cache) UpdateVolumeWithTX(tx core.Transaction) {
	for _, posting := range tx.Postings {
		c.addOutput(posting.Source, posting.Asset, posting.Amount)
		c.addInput(posting.Destination, posting.Asset, posting.Amount)
	}
}

func (c *Cache) UpdateAccountMetadata(address string, m core.Metadata) error {
	c.withLockOnAccount(address, func(account *core.AccountWithVolumes) {
		account.Metadata = account.Metadata.Merge(m)
	})

	return nil
}

func New(store storage.LedgerStore) *Cache {
	return &Cache{
		store: store,
		//TODO(gfyrag): Make configurable
		cache: gcache.New(1000).LFU().Build(),
	}
}
