package noopstorage

import (
	"context"
	"errors"

	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/storage"
)

var (
	ErrConfigurationNotFound = errors.New("configuration not found")
)

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
