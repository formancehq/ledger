package storage

import (
	"context"
)

type Factory interface {
	GetStore(ctx context.Context, name string) (Store, error)
	Close(ctx context.Context) error
}

type BuiltInFactory struct {
	Driver Driver
}

func (f *BuiltInFactory) GetStore(ctx context.Context, name string) (Store, error) {
	return f.Driver.NewStore(ctx, name)
}

func (f *BuiltInFactory) Close(ctx context.Context) error {
	return f.Driver.Close(ctx)
}

func NewDefaultFactory(driver Driver) Factory {
	return &BuiltInFactory{Driver: driver}
}

type noOpFactory struct{}

func (f *noOpFactory) GetStore(ctx context.Context, name string) (Store, error) {
	return nil, nil
}

func (f *noOpFactory) Close(ctx context.Context) error {
	return nil
}

func NoOpFactory() Factory {
	return &noOpFactory{}
}
