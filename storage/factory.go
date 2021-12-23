package storage

import (
	"context"
)

type Factory interface {
	GetStore(name string) (Store, error)
	Close(ctx context.Context) error
}

type BuiltInFactory struct {
	Driver Driver
}

func (f *BuiltInFactory) GetStore(name string) (Store, error) {
	return f.Driver.NewStore(name)
}

func (f *BuiltInFactory) Close(ctx context.Context) error {
	return f.Driver.Close(ctx)
}

func NewDefaultFactory(driver Driver) Factory {
	return &BuiltInFactory{Driver: driver}
}
