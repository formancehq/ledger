package storage

import (
	"context"
)

type Driver interface {
	Initialize(ctx context.Context) error
	GetLedgerStore(ctx context.Context, name string, create bool) (LedgerStore, bool, error)
	DeleteLedgerStore(ctx context.Context, name string) error
	GetSystemStore(ctx context.Context) (SystemStore, error)
	Close(ctx context.Context) error
	Name() string
}

type noOpDriver struct{}

func (n noOpDriver) Register(ctx context.Context, ledger string) (bool, error) {
	return false, nil
}

func (n noOpDriver) Exists(ctx context.Context, ledger string) (bool, error) {
	return false, nil
}

func (n noOpDriver) Delete(ctx context.Context, ledger string) error {
	return nil
}

func (n noOpDriver) GetSystemStore(ctx context.Context) (SystemStore, error) {
	return n, nil
}

func (n noOpDriver) DeleteLedgerStore(ctx context.Context, name string) error {
	return nil
}

func (n noOpDriver) Initialize(ctx context.Context) error {
	return nil
}

func (n noOpDriver) GetLedgerStore(ctx context.Context, name string, create bool) (LedgerStore, bool, error) {
	return nil, false, nil
}

func (n noOpDriver) Close(ctx context.Context) error {
	return nil
}

func (n noOpDriver) List(ctx context.Context) ([]string, error) {
	return nil, nil
}

func (n noOpDriver) Name() string {
	return ""
}

var _ Driver = &noOpDriver{}

func NoOpDriver() *noOpDriver {
	return &noOpDriver{}
}
