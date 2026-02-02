package store

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Cursor provides a way to iterate over a stream of items
type Cursor[T any] interface {
	// Next returns the next item in the cursor
	// Returns io.EOF when there are no more items
	Next() (T, error)
	// Close closes the cursor and releases any resources
	Close() error
}

type GRPCStreamCursor[Res, To any] struct {
	client grpc.ServerStreamingClient[Res]
	mapper func(*Res) (To, error)
}

func (cursor GRPCStreamCursor[Res, To]) Next() (To, error) {
	next, err := cursor.client.Recv()
	if err != nil {
		if status.Code(err) == codes.Canceled {
			err = context.Canceled
		}
		var zero To
		return zero, err
	}
	return cursor.mapper(next)
}

func (cursor GRPCStreamCursor[Res, To]) Close() error {
	return cursor.client.CloseSend()
}

var _ Cursor[any] = (*GRPCStreamCursor[any, any])(nil)

func NewGRPCStreamCursor[Res, To any](client grpc.ServerStreamingClient[Res], mapper func(*Res) (To, error)) Cursor[To] {
	return GRPCStreamCursor[Res, To]{client: client, mapper: mapper}
}

// SliceCursor wraps a slice to implement the Cursor interface
type SliceCursor[T any] struct {
	items []T
	index int
}

func (c *SliceCursor[T]) Next() (T, error) {
	if c.index >= len(c.items) {
		var zero T
		return zero, io.EOF
	}
	item := c.items[c.index]
	c.index++
	return item, nil
}

func (c *SliceCursor[T]) Close() error {
	return nil
}

var _ Cursor[any] = (*SliceCursor[any])(nil)

// NewSliceCursor creates a new cursor from a slice
func NewSliceCursor[T any](items []T) Cursor[T] {
	return &SliceCursor[T]{items: items, index: 0}
}
