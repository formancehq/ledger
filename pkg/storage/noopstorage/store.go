package noopstorage

import (
	"context"
	"time"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
)

// A no op store. Useful for testing.
type noOpStore struct{}

func (n noOpStore) WithTX(ctx context.Context, callback func(api ledger.API) error) error {
	return nil
}

func (n noOpStore) UpdateTransactionMetadata(ctx context.Context, id uint64, metadata core.Metadata, at time.Time) error {
	return nil
}

func (n noOpStore) UpdateAccountMetadata(ctx context.Context, id string, metadata core.Metadata, at time.Time) error {
	return nil
}

func (n noOpStore) Commit(ctx context.Context, txs ...core.ExpandedTransaction) error {
	return nil
}

func (n noOpStore) GetVolumes(ctx context.Context, accountAddress, asset string) (core.Volumes, error) {
	return core.Volumes{}, nil
}

func (n noOpStore) GetLastTransaction(ctx context.Context) (*core.ExpandedTransaction, error) {
	return &core.ExpandedTransaction{}, nil
}

func (n noOpStore) Logs(ctx context.Context) ([]core.Log, error) {
	return nil, nil
}

func (n noOpStore) CountTransactions(ctx context.Context, q ledger.TransactionsQuery) (uint64, error) {
	return 0, nil
}

func (n noOpStore) GetTransactions(ctx context.Context, q ledger.TransactionsQuery) (sharedapi.Cursor[core.ExpandedTransaction], error) {
	return sharedapi.Cursor[core.ExpandedTransaction]{}, nil
}

func (n noOpStore) GetTransaction(ctx context.Context, txid uint64) (*core.ExpandedTransaction, error) {
	return nil, nil
}

func (n noOpStore) GetAccount(ctx context.Context, accountAddress string) (*core.Account, error) {
	return nil, nil
}

func (n noOpStore) GetAssetsVolumes(ctx context.Context, accountAddress string) (core.AssetsVolumes, error) {
	return nil, nil
}

func (n noOpStore) LastLog(ctx context.Context) (*core.Log, error) {
	return nil, nil
}

func (n noOpStore) CountAccounts(ctx context.Context, q ledger.AccountsQuery) (uint64, error) {
	return 0, nil
}

func (n noOpStore) GetAccounts(ctx context.Context, q ledger.AccountsQuery) (sharedapi.Cursor[core.Account], error) {
	return sharedapi.Cursor[core.Account]{}, nil
}

func (n noOpStore) GetBalances(ctx context.Context, q ledger.BalancesQuery) (sharedapi.Cursor[core.AccountsBalances], error) {
	return sharedapi.Cursor[core.AccountsBalances]{}, nil
}

func (n noOpStore) GetBalancesAggregated(ctx context.Context, q ledger.BalancesQuery) (core.AssetsBalances, error) {
	return core.AssetsBalances{}, nil
}

func (n noOpStore) CountMeta(ctx context.Context) (int64, error) {
	return 0, nil
}

func (n noOpStore) Initialize(ctx context.Context) (bool, error) {
	return false, nil
}

func (n noOpStore) LoadMapping(context.Context) (*core.Mapping, error) {
	return nil, nil
}

func (n noOpStore) SaveMapping(ctx context.Context, mapping core.Mapping) error {
	return nil
}

func (n noOpStore) Name() string {
	return "noop"
}

func (n noOpStore) Close(ctx context.Context) error {
	return nil
}

var _ ledger.Store = &noOpStore{}

func NoOpStore() *noOpStore {
	return &noOpStore{}
}
