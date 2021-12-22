package storage

import (
	"context"
)

type Driver interface {
	Initialize(ctx context.Context) error
	NewStore(name string) (Store, error)
	Close(ctx context.Context) error
}

var drivers = make(map[string]Driver)

func RegisterDriver(name string, driver Driver) {
	drivers[name] = driver
}
