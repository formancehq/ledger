package storage

import (
	"context"
	"sync"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/stack/libs/go-libs/collectionutils"
	"github.com/formancehq/stack/libs/go-libs/metadata"
)

type InMemoryStore struct {
	mu sync.Mutex

	Logs         []*core.ChainedLog
	Accounts     map[string]*core.AccountWithVolumes
	Transactions []*core.ExpandedTransaction
}

func (m *InMemoryStore) MarkedLogsAsProjected(ctx context.Context, id uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, log := range m.Logs {
		if log.ID == id {
			log.Projected = true
			return nil
		}
	}
	return nil
}

func (m *InMemoryStore) InsertMoves(ctx context.Context, insert ...*core.Move) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	// TODO(gfyrag): to reflect the behavior of the real storage, we should compute accounts volumes there
	return nil
}

func (m *InMemoryStore) UpdateAccountsMetadata(ctx context.Context, update ...core.Account) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, account := range update {
		persistedAccount, ok := m.Accounts[account.Address]
		if !ok {
			m.Accounts[account.Address] = &core.AccountWithVolumes{
				Account: account,
				Volumes: core.VolumesByAssets{},
			}
			return nil
		}
		persistedAccount.Metadata = persistedAccount.Metadata.Merge(account.Metadata)
	}
	return nil
}

func (m *InMemoryStore) InsertTransactions(ctx context.Context, insert ...core.Transaction) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, transaction := range insert {
		expandedTransaction := &core.ExpandedTransaction{
			Transaction:       transaction,
			PreCommitVolumes:  core.AccountsAssetsVolumes{},
			PostCommitVolumes: core.AccountsAssetsVolumes{},
		}
		for _, posting := range transaction.Postings {
			account, ok := m.Accounts[posting.Source]
			if !ok {
				account = core.NewAccountWithVolumes(posting.Source)
				m.Accounts[posting.Source] = account
			}

			asset, ok := account.Volumes[posting.Asset]
			if !ok {
				asset = core.NewEmptyVolumes()
				account.Volumes[posting.Asset] = asset
			}

			account, ok = m.Accounts[posting.Destination]
			if !ok {
				account = core.NewAccountWithVolumes(posting.Destination)
				m.Accounts[posting.Destination] = account
			}

			asset, ok = account.Volumes[posting.Asset]
			if !ok {
				asset = core.NewEmptyVolumes()
				account.Volumes[posting.Asset] = asset
			}
		}
		for _, posting := range transaction.Postings {
			expandedTransaction.PreCommitVolumes.AddOutput(posting.Source, posting.Asset,
				m.Accounts[posting.Source].Volumes[posting.Asset].Output)
			expandedTransaction.PreCommitVolumes.AddInput(posting.Source, posting.Asset,
				m.Accounts[posting.Source].Volumes[posting.Asset].Input)

			expandedTransaction.PreCommitVolumes.AddOutput(posting.Destination, posting.Asset,
				m.Accounts[posting.Destination].Volumes[posting.Asset].Output)
			expandedTransaction.PreCommitVolumes.AddInput(posting.Destination, posting.Asset,
				m.Accounts[posting.Destination].Volumes[posting.Asset].Input)
		}
		for _, posting := range transaction.Postings {
			account := m.Accounts[posting.Source]
			asset := account.Volumes[posting.Asset]
			asset.Output = asset.Output.Add(asset.Output, posting.Amount)

			account = m.Accounts[posting.Destination]
			asset = account.Volumes[posting.Asset]
			asset.Input = asset.Input.Add(asset.Input, posting.Amount)
		}
		for _, posting := range transaction.Postings {
			expandedTransaction.PostCommitVolumes.AddOutput(posting.Source, posting.Asset,
				m.Accounts[posting.Source].Volumes[posting.Asset].Output)
			expandedTransaction.PostCommitVolumes.AddInput(posting.Source, posting.Asset,
				m.Accounts[posting.Source].Volumes[posting.Asset].Input)

			expandedTransaction.PostCommitVolumes.AddOutput(posting.Destination, posting.Asset,
				m.Accounts[posting.Destination].Volumes[posting.Asset].Output)
			expandedTransaction.PostCommitVolumes.AddInput(posting.Destination, posting.Asset,
				m.Accounts[posting.Destination].Volumes[posting.Asset].Input)
		}

		m.Transactions = append(m.Transactions, expandedTransaction)
	}
	return nil
}

func (m *InMemoryStore) UpdateTransactionsMetadata(ctx context.Context, update ...core.TransactionWithMetadata) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, tx := range update {
		m.Transactions[tx.ID].Metadata = m.Transactions[tx.ID].Metadata.Merge(tx.Metadata)
	}
	return nil
}

func (m *InMemoryStore) EnsureAccountsExist(ctx context.Context, accounts []string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, address := range accounts {
		_, ok := m.Accounts[address]
		if ok {
			continue
		}
		m.Accounts[address] = &core.AccountWithVolumes{
			Account: core.Account{
				Address:  address,
				Metadata: metadata.Metadata{},
			},
			Volumes: core.VolumesByAssets{},
		}
	}
	return nil
}

func (m *InMemoryStore) IsInitialized() bool {
	return true
}

func (m *InMemoryStore) GetNextLogID(ctx context.Context) (uint64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, log := range m.Logs {
		if !log.Projected {
			return log.ID, nil
		}
	}
	return uint64(len(m.Logs)), nil
}

func (m *InMemoryStore) ReadLogsRange(ctx context.Context, idMin, idMax uint64) ([]core.ChainedLog, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if idMax > uint64(len(m.Logs)) {
		idMax = uint64(len(m.Logs))
	}

	if idMin < uint64(len(m.Logs)) {
		return collectionutils.Map(m.Logs[idMin:idMax], func(from *core.ChainedLog) core.ChainedLog {
			return *from
		}), nil
	}

	return []core.ChainedLog{}, nil
}

func (m *InMemoryStore) GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	account, ok := m.Accounts[address]
	if !ok {
		return &core.AccountWithVolumes{
			Account: core.Account{
				Address:  address,
				Metadata: metadata.Metadata{},
			},
			Volumes: core.VolumesByAssets{},
		}, nil
	}
	return account, nil
}

func (m *InMemoryStore) GetTransaction(ctx context.Context, id uint64) (*core.ExpandedTransaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.Transactions[id], nil
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		Accounts: make(map[string]*core.AccountWithVolumes),
	}
}
