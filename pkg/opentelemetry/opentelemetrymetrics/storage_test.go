package opentelemetrymetrics

import (
	"context"
	"testing"
	"time"

	"github.com/numary/go-libs/sharedapi"
	"github.com/numary/ledger/pkg/core"
	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
)

// A no op store. Useful for testing.
type noOpStore struct{}

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

type noOpDriver struct{}

func (n noOpDriver) DeleteLedger(ctx context.Context, name string) error {
	return nil
}

func (n noOpDriver) GetSystemStore() storage.SystemStore {
	return n
}

func (n noOpDriver) GetLedgerStore(ctx context.Context, name string, create bool) (ledger.Store, bool, error) {
	return noOpStore{}, false, nil
}

func (n noOpDriver) InsertConfiguration(ctx context.Context, key, value string) error {
	return nil
}

func (n noOpDriver) GetConfiguration(ctx context.Context, key string) (string, error) {
	return "", nil
}

func (n noOpDriver) DeleteStore(ctx context.Context, name string) error {
	return nil
}

func (n noOpDriver) Initialize(ctx context.Context) error {
	return nil
}

func (n noOpDriver) Close(ctx context.Context) error {
	return nil
}

func (n noOpDriver) ListLedgers(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (n noOpDriver) Name() string {
	return ""
}

var _ storage.Driver[ledger.Store] = &noOpDriver{}

func NoOpDriver() *noOpDriver {
	return &noOpDriver{}
}

func TestWrapStorageFactory(t *testing.T) {
	f := WrapStorageDriver(NoOpDriver(), metric.NewNoopMeterProvider())
	store, _, err := f.GetLedgerStore(context.Background(), "bar", true)
	assert.NoError(t, err)
	assert.NotNil(t, store)
	assert.IsType(t, new(storageDecorator), store)
}

func TestNewStorageDecorator(t *testing.T) {
	m := global.Meter("foo")

	transactionsCounter, err := transactionsCounter(m)
	assert.NoError(t, err)

	store := NewStorageDecorator(&noOpStore{}, transactionsCounter)
	assert.NotNil(t, store)
	assert.IsType(t, new(storageDecorator), store)

	err = store.Commit(context.Background(), core.ExpandedTransaction{})
	assert.NoError(t, err)
}
