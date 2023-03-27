package cache

import (
	"context"
	"math/big"
	"strings"
	"sync"

	"github.com/bluele/gcache"
	"github.com/formancehq/ledger/pkg/core"
	"golang.org/x/sync/singleflight"
)

type AccountComputer interface {
	ComputeAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error)
}
type AccountComputerFn func(ctx context.Context, address string) (*core.AccountWithVolumes, error)

func (fn AccountComputerFn) ComputeAccount(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	return fn(ctx, address)
}

type cacheEntry struct {
	sync.Mutex
	account *core.AccountWithVolumes
}

type Cache struct {
	cache           gcache.Cache
	accountComputer AccountComputer
	sg              singleflight.Group
}

func (c *Cache) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {

	address = strings.TrimPrefix(address, "@")

	item, err, _ := c.sg.Do(address, func() (interface{}, error) {
		item, err := c.cache.Get(address)
		if err == nil {
			return item, nil
		}
		entry := &cacheEntry{}

		// TODO: Rename later ?
		entry.account, err = c.accountComputer.ComputeAccount(ctx, address)
		if err != nil {
			return nil, err
		}
		if err := c.cache.Set(address, entry); err != nil {
			return nil, err
		}
		return entry, nil
	})
	if err != nil {
		panic(err)
	}
	cp := item.(*cacheEntry).account.Copy()

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

func (c *Cache) addOutput(address, asset string, amount *big.Int) {
	c.withLockOnAccount(address, func(account *core.AccountWithVolumes) {
		volumes := account.Volumes[asset].CopyWithZerosIfNeeded()
		volumes.Output.Add(volumes.Output, amount)
		account.Volumes[asset] = volumes
	})
}

func (c *Cache) addInput(address, asset string, amount *big.Int) {
	c.withLockOnAccount(address, func(account *core.AccountWithVolumes) {
		volumes := account.Volumes[asset].CopyWithZerosIfNeeded()
		volumes.Input.Add(volumes.Input, amount)
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

func New(accountComputer AccountComputer) *Cache {
	return &Cache{
		accountComputer: accountComputer,
		//TODO(gfyrag): Make configurable
		cache: gcache.New(1024).LFU().Build(),
	}
}
