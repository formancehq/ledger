package cache

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/storage"
)

type Manager struct {
	mu            sync.Mutex
	storageDriver storage.Driver
	// TODO(gfyrag): In a future release, we should be able to clear old cache from memory
	ledgers map[string]*Cache
}

func (m *Manager) ForLedger(ctx context.Context, ledger string) (*Cache, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	cache, ok := m.ledgers[ledger]
	if !ok {
		store, _, err := m.storageDriver.GetLedgerStore(ctx, ledger, true)
		if err != nil {
			return nil, err
		}
		cache = New(store)
		m.ledgers[ledger] = cache
	}
	return cache, nil
}

func NewManager(storageDriver storage.Driver) *Manager {
	return &Manager{
		mu:            sync.Mutex{},
		storageDriver: storageDriver,
		ledgers:       map[string]*Cache{},
	}
}
