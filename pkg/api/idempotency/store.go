package idempotency

import (
	"context"
	"errors"
)

var (
	ErrIKNotFound = errors.New("not found")
)

type Store interface {
	CreateIK(ctx context.Context, key string, response Response) error
	ReadIK(ctx context.Context, key string) (*Response, error)
}

type inMemoryStore struct {
	iks map[string]Response
}

func (i *inMemoryStore) CreateIK(ctx context.Context, key string, response Response) error {
	i.iks[key] = response
	return nil
}

func (i *inMemoryStore) ReadIK(ctx context.Context, key string) (*Response, error) {
	response, ok := i.iks[key]
	if !ok {
		return nil, ErrIKNotFound
	}
	return &response, nil
}

var _ Store = &inMemoryStore{}

func NewInMemoryStore() *inMemoryStore {
	return &inMemoryStore{
		iks: map[string]Response{},
	}
}
