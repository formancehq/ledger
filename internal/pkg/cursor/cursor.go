package cursor

import (
	"errors"
	"io"
)

// Cursor provides a way to iterate over a stream of items.
type Cursor[T any] interface {
	// Next returns the next item in the cursor
	// Returns io.EOF when there are no more items
	Next() (T, error)
	// Close closes the cursor and releases any resources
	Close() error
}

// SliceCursor wraps a slice to implement the Cursor interface.
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

// NewSliceCursor creates a new cursor from a slice.
func NewSliceCursor[T any](items []T) Cursor[T] {
	return &SliceCursor[T]{items: items, index: 0}
}

// FilteredCursor wraps a cursor and filters items based on a predicate.
type FilteredCursor[T any] struct {
	inner     Cursor[T]
	predicate func(T) bool
}

func (c *FilteredCursor[T]) Next() (T, error) {
	for {
		item, err := c.inner.Next()
		if err != nil {
			var zero T

			return zero, err
		}

		if c.predicate(item) {
			return item, nil
		}
		// Skip items that don't match the predicate
	}
}

func (c *FilteredCursor[T]) Close() error {
	return c.inner.Close()
}

var _ Cursor[any] = (*FilteredCursor[any])(nil)

// NewFilteredCursor creates a new cursor that filters items based on a predicate.
func NewFilteredCursor[T any](inner Cursor[T], predicate func(T) bool) Cursor[T] {
	return &FilteredCursor[T]{inner: inner, predicate: predicate}
}

// FilteredCursorE wraps a cursor and filters items based on a predicate that may
// return an error. A predicate error aborts iteration and is returned from Next.
type FilteredCursorE[T any] struct {
	inner     Cursor[T]
	predicate func(T) (bool, error)
}

func (c *FilteredCursorE[T]) Next() (T, error) {
	for {
		item, err := c.inner.Next()
		if err != nil {
			var zero T

			return zero, err
		}
		ok, perr := c.predicate(item)
		if perr != nil {
			var zero T

			return zero, perr
		}
		if ok {
			return item, nil
		}
	}
}

func (c *FilteredCursorE[T]) Close() error { return c.inner.Close() }

var _ Cursor[any] = (*FilteredCursorE[any])(nil)

// NewFilteredCursorE creates a filtering cursor whose predicate may fail; the
// failure is surfaced from Next rather than silently dropping the item.
func NewFilteredCursorE[T any](inner Cursor[T], predicate func(T) (bool, error)) Cursor[T] {
	return &FilteredCursorE[T]{inner: inner, predicate: predicate}
}

// LimitedCursor wraps a cursor and limits the number of items returned.
type LimitedCursor[T any] struct {
	inner    Cursor[T]
	limit    uint32
	returned uint32
}

func (c *LimitedCursor[T]) Next() (T, error) {
	if c.returned >= c.limit {
		var zero T

		return zero, io.EOF
	}

	item, err := c.inner.Next()
	if err != nil {
		var zero T

		return zero, err
	}

	c.returned++

	return item, nil
}

func (c *LimitedCursor[T]) Close() error {
	return c.inner.Close()
}

var _ Cursor[any] = (*LimitedCursor[any])(nil)

// NewLimitedCursor creates a new cursor that returns at most limit items.
func NewLimitedCursor[T any](inner Cursor[T], limit uint32) Cursor[T] {
	return &LimitedCursor[T]{inner: inner, limit: limit}
}

// ClosingCursor wraps a cursor and closes additional resources on Close.
type ClosingCursor[T any] struct {
	inner  Cursor[T]
	closer io.Closer
}

func (c *ClosingCursor[T]) Next() (T, error) {
	return c.inner.Next()
}

func (c *ClosingCursor[T]) Close() error {
	err := c.inner.Close()
	if closeErr := c.closer.Close(); err == nil {
		err = closeErr
	}

	return err
}

// NewClosingCursor creates a cursor that closes the given io.Closer when the cursor is closed.
func NewClosingCursor[T any](inner Cursor[T], closer io.Closer) Cursor[T] {
	return &ClosingCursor[T]{inner: inner, closer: closer}
}

// SkipWhileCursor wraps a cursor and discards items at the start while the
// predicate returns true. Once the predicate becomes false, the wrapped cursor
// passes every remaining item through.
type SkipWhileCursor[T any] struct {
	inner     Cursor[T]
	predicate func(T) bool
	skipped   bool
}

func (c *SkipWhileCursor[T]) Next() (T, error) {
	if c.skipped {
		return c.inner.Next()
	}

	for {
		item, err := c.inner.Next()
		if err != nil {
			var zero T

			return zero, err
		}

		if !c.predicate(item) {
			c.skipped = true

			return item, nil
		}
	}
}

func (c *SkipWhileCursor[T]) Close() error {
	return c.inner.Close()
}

var _ Cursor[any] = (*SkipWhileCursor[any])(nil)

// NewSkipWhileCursor creates a cursor that drops a contiguous prefix of items
// for which predicate returns true. Useful for resuming after a string/uint64
// cursor (skip items whose key <= cursor in ascending iteration).
func NewSkipWhileCursor[T any](inner Cursor[T], predicate func(T) bool) Cursor[T] {
	return &SkipWhileCursor[T]{inner: inner, predicate: predicate}
}

// Reverse drains the cursor, reverses the items in place, and returns a fresh
// slice cursor. The wrapped cursor is closed.
//
// Suitable for small to medium in-memory collections (a few thousand items at
// most). Large or unbounded streams should use a backend-specific reverse
// iteration mechanism instead.
func Reverse[T any](c Cursor[T]) (Cursor[T], error) {
	items, err := Collect(c)
	if err != nil {
		return nil, err
	}

	for i, j := 0, len(items)-1; i < j; i, j = i+1, j-1 {
		items[i], items[j] = items[j], items[i]
	}

	return NewSliceCursor(items), nil
}

// Collect drains a cursor into a slice and closes it.
func Collect[T any](c Cursor[T]) ([]T, error) {
	defer func() { _ = c.Close() }()

	var items []T

	for {
		item, err := c.Next()
		if errors.Is(err, io.EOF) {
			return items, nil
		}

		if err != nil {
			return nil, err
		}

		items = append(items, item)
	}
}
