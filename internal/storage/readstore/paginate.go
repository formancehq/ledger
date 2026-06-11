package readstore

import "bytes"

// PaginateForward collects up to pageSize entity IDs from a forward
// (ascending) EntityIterator. If after is non-nil, the iterator is positioned
// past that entity before collecting. Returns the collected items, whether
// more items exist beyond the page, and any storage error surfaced via the
// iterator (#320). A non-nil error MUST be propagated to the caller — silently
// returning a short page would deliver a truncated balance/transaction list
// as if it were complete.
func PaginateForward(iter EntityIterator, pageSize uint32, after []byte) (items [][]byte, hasMore bool, err error) {
	var positioned bool
	if after != nil {
		positioned = iter.SeekGE(after)
		// Skip the cursor entity itself (we want items after it)
		if positioned && bytes.Equal(iter.Current(), after) {
			positioned = iter.Next()
		}
	} else {
		positioned = iter.Next()
	}

	if !positioned {
		return nil, false, iter.Err()
	}

	items, hasMore = collectPage(iter, pageSize)

	return items, hasMore, iter.Err()
}

// ReverseIterator is the interface for reverse (descending) iteration.
// Err() reports the first storage error encountered. Callers MUST consult
// Err after Next/SeekLE returns false to distinguish clean exhaustion from
// an I/O failure (#320).
type ReverseIterator interface {
	Next() bool
	Current() []byte
	SeekLE(target []byte) bool
	Err() error
}

// PaginateReverse collects up to pageSize entity IDs from a
// ReverseIterator (descending order). If before is non-nil, the iterator
// is positioned at the first entity <= before and that entity is skipped.
// Returns the collected items, whether more items exist, and any storage
// error surfaced via the iterator. See PaginateForward for the rationale
// (#320).
func PaginateReverse(iter ReverseIterator, pageSize uint32, before []byte) (items [][]byte, hasMore bool, err error) {
	var positioned bool
	if before != nil {
		positioned = iter.SeekLE(before)
		// Skip the cursor entity itself
		if positioned && bytes.Equal(iter.Current(), before) {
			positioned = iter.Next()
		}
	} else {
		positioned = iter.Next()
	}

	if !positioned {
		return nil, false, iter.Err()
	}

	items, hasMore = collectPage(iter, pageSize)

	return items, hasMore, iter.Err()
}

// nextable is the common subset of EntityIterator and ReversePrefixIterator.
type nextable interface {
	Next() bool
	Current() []byte
}

// collectPage collects up to pageSize+1 items from an iterator that is already
// positioned on the first item. If more than pageSize items are available,
// hasMore is true and only pageSize items are returned.
func collectPage(iter nextable, pageSize uint32) (items [][]byte, hasMore bool) {
	limit := int(pageSize) + 1

	for {
		cp := make([]byte, len(iter.Current()))
		copy(cp, iter.Current())

		items = append(items, cp)
		if len(items) >= limit {
			break
		}

		if !iter.Next() {
			break
		}
	}

	hasMore = len(items) > int(pageSize)
	if hasMore {
		items = items[:pageSize]
	}

	return items, hasMore
}
