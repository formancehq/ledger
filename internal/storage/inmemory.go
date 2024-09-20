package storage

import (
	"context"
	"math/big"

	"github.com/formancehq/ledger/internal/storage/sqlutils"

	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
)

type InMemoryStore struct {
	logs         []*ledger.ChainedLog
	transactions []*ledger.ExpandedTransaction
	accounts     []*ledger.Account
}

func (m *InMemoryStore) GetTransactionByReference(ctx context.Context, ref string) (*ledger.ExpandedTransaction, error) {
	filtered := collectionutils.Filter(m.transactions, func(transaction *ledger.ExpandedTransaction) bool {
		return transaction.Reference == ref
	})
	if len(filtered) == 0 {
		return nil, sqlutils.ErrNotFound
	}
	return filtered[0], nil
}

func (m *InMemoryStore) GetTransaction(ctx context.Context, txID *big.Int) (*ledger.Transaction, error) {
	filtered := collectionutils.Filter(m.transactions, func(transaction *ledger.ExpandedTransaction) bool {
		return transaction.ID.Cmp(txID) == 0
	})
	if len(filtered) == 0 {
		return nil, sqlutils.ErrNotFound
	}
	return &filtered[0].Transaction, nil
}

func (m *InMemoryStore) GetLastLog(ctx context.Context) (*ledger.ChainedLog, error) {
	if len(m.logs) == 0 {
		return nil, nil
	}
	return m.logs[len(m.logs)-1], nil
}

func (m *InMemoryStore) GetBalance(ctx context.Context, address, asset string) (*big.Int, error) {
	balance := new(big.Int)

	var processPostings = func(postings ledger.Postings) {
		for _, posting := range postings {
			if posting.Asset != asset {
				continue
			}
			if posting.Source == address {
				balance = balance.Sub(balance, posting.Amount)
			}
			if posting.Destination == address {
				balance = balance.Add(balance, posting.Amount)
			}
		}
	}

	for _, log := range m.logs {
		switch payload := log.Data.(type) {
		case ledger.NewTransactionLogPayload:
			processPostings(payload.Transaction.Postings)
		case ledger.RevertedTransactionLogPayload:
			processPostings(payload.RevertTransaction.Postings)
		}
	}
	return balance, nil
}

func (m *InMemoryStore) GetAccount(ctx context.Context, address string) (*ledger.Account, error) {
	account := collectionutils.Filter(m.accounts, func(account *ledger.Account) bool {
		return account.Address == address
	})
	if len(account) == 0 {
		return &ledger.Account{
			Address:  address,
			Metadata: metadata.Metadata{},
		}, nil
	}
	return account[0], nil
}

func (m *InMemoryStore) ReadLogWithIdempotencyKey(ctx context.Context, key string) (*ledger.ChainedLog, error) {
	first := collectionutils.First(m.logs, func(log *ledger.ChainedLog) bool {
		return log.IdempotencyKey == key
	})
	if first == nil {
		return nil, sqlutils.ErrNotFound
	}
	return first, nil
}

func (m *InMemoryStore) InsertLogs(ctx context.Context, logs ...*ledger.ChainedLog) error {

	m.logs = append(m.logs, logs...)
	for _, log := range logs {
		switch payload := log.Data.(type) {
		case ledger.NewTransactionLogPayload:
			m.transactions = append(m.transactions, &ledger.ExpandedTransaction{
				Transaction: *payload.Transaction,
				// TODO
				PreCommitVolumes:  nil,
				PostCommitVolumes: nil,
			})
		case ledger.RevertedTransactionLogPayload:
			tx := collectionutils.Filter(m.transactions, func(transaction *ledger.ExpandedTransaction) bool {
				return transaction.ID.Cmp(payload.RevertedTransactionID) == 0
			})[0]
			tx.Reverted = true
			m.transactions = append(m.transactions, &ledger.ExpandedTransaction{
				Transaction: *payload.RevertTransaction,
				// TODO
				PreCommitVolumes:  nil,
				PostCommitVolumes: nil,
			})
		case ledger.SetMetadataLogPayload:
		}
	}

	return nil
}

func (m *InMemoryStore) GetLastTransaction(ctx context.Context) (*ledger.ExpandedTransaction, error) {
	if len(m.transactions) == 0 {
		return nil, sqlutils.ErrNotFound
	}
	return m.transactions[len(m.transactions)-1], nil
}

func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		logs: []*ledger.ChainedLog{},
	}
}
