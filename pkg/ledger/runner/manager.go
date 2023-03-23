package runner

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/ledger/cache"
	"github.com/formancehq/ledger/pkg/ledger/lock"
	"github.com/formancehq/ledger/pkg/ledger/numscript"
	"github.com/formancehq/ledger/pkg/storage"
)

// TODO(gfyrag): In a future release, we should be able to clear old runners from memory
type Manager struct {
	mu                  sync.Mutex
	storageDriver       storage.Driver
	lock                lock.Locker
	allowPastTimestamps bool
	cacheManager        *cache.Manager
	compiler            *numscript.Compiler
	// ledgers store the script runner for each ledger
	ledgers map[string]*Runner
}

func (m *Manager) ForLedger(ctx context.Context, ledger string) (*Runner, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	runner, ok := m.ledgers[ledger]
	if !ok {
		store, _, err := m.storageDriver.GetLedgerStore(ctx, ledger, true)
		if err != nil {
			return nil, err
		}

		cache, err := m.cacheManager.ForLedger(ctx, ledger)
		if err != nil {
			return nil, err
		}

		runner, err = New(store, m.lock, cache, m.compiler, m.allowPastTimestamps)
		if err != nil {
			return nil, err
		}
		m.ledgers[ledger] = runner
	}
	return runner, nil
}

func NewManager(storageDriver storage.Driver, lock lock.Locker, cacheManager *cache.Manager, allowPastTimestamps bool) *Manager {
	return &Manager{
		storageDriver:       storageDriver,
		lock:                lock,
		allowPastTimestamps: allowPastTimestamps,
		cacheManager:        cacheManager,
		ledgers:             map[string]*Runner{},
		compiler:            numscript.NewCompiler(),
	}
}
