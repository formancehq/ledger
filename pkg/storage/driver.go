package storage

import (
	"context"
)

type Driver interface {
	Initialize(ctx context.Context) error
	GetStore(ctx context.Context, name string, create bool) (Store, bool, error)
	Close(ctx context.Context) error
	List(ctx context.Context) ([]string, error)
	DeleteStore(ctx context.Context, name string) error
	CleanTablesFromLedger(ctx context.Context, ledger string) error
	Name() string
}

type noOpDriver struct{}

func (n noOpDriver) DeleteStore(ctx context.Context, name string) error {
	return nil
}

func (n noOpDriver) CleanTablesFromLedger(ctx context.Context, ledger string) error {
	return nil
}

func (n noOpDriver) Initialize(ctx context.Context) error {
	return nil
}

func (n noOpDriver) GetStore(ctx context.Context, name string, create bool) (Store, bool, error) {
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
