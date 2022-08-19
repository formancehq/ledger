package storage

import (
	"context"
	"errors"
)

var (
	ErrConfigurationNotFound = errors.New("configuration not found")
)

type Driver interface {
	Initialize(ctx context.Context) error
	GetStore(ctx context.Context, name string, create bool) (Store, bool, error)
	Close(ctx context.Context) error
	List(ctx context.Context) ([]string, error)
	DeleteStore(ctx context.Context, name string) error
	Name() string
	GetConfiguration(ctx context.Context, key string) (string, error)
	InsertConfiguration(ctx context.Context, key, value string) error
}

type noOpDriver struct{}

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
