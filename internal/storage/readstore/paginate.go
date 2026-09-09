package readstore

import "bytes"

// paginate collects up to pageSize entity IDs from iter in D's order. If
// cursor is non-nil, the iterator is positioned at the first entity at or
// after it and that entity is skipped, so the page starts strictly past the
// cursor.
//
// Returns the collected items, whether more items exist beyond the page, and
// any storage error surfaced via the iterator (#320). A non-nil error MUST be
// propagated to the caller — silently returning a short page would deliver a
// truncated balance/transaction list as if it were complete.
func paginate[D Direction](iter Iterator[D], pageSize uint32, cursor []byte) (items [][]byte, hasMore bool, err error) {
	var positioned bool
	if cursor != nil {
		positioned = iter.Seek(cursor)
		// Skip the cursor entity itself: the page starts after it.
		if positioned && bytes.Equal(iter.Current(), cursor) {
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

// PaginateForward collects a page from an ascending iterator, starting past
// after when it is non-nil.
func PaginateForward(iter EntityIterator, pageSize uint32, after []byte) (items [][]byte, hasMore bool, err error) {
	return paginate[Asc](iter, pageSize, after)
}

// PaginateReverse collects a page from a descending iterator, starting past
// before when it is non-nil.
func PaginateReverse(iter ReverseIterator, pageSize uint32, before []byte) (items [][]byte, hasMore bool, err error) {
	return paginate[Desc](iter, pageSize, before)
}

// nextable is the positioning subset every Iterator shares, in either
// direction.
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
