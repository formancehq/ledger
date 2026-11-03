package query

import (
	"encoding/binary"
	"fmt"
	"strconv"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/pkg/pagecursor"
)

const (
	// DefaultPageSize is the user-visible page size when callers pass zero.
	DefaultPageSize uint32 = 100
	// MaxPageSize bounds user-visible list pages before collection and enrichment.
	MaxPageSize uint32 = 1000
)

// ClampPageSize applies the default and upper bound to a user-visible list page.
// Internal fetches may need an additional lookahead item; aggregate queries do
// not use this policy.
func ClampPageSize(pageSize uint32) uint32 {
	if pageSize == 0 {
		return DefaultPageSize
	}
	if pageSize > MaxPageSize {
		return MaxPageSize
	}

	return pageSize
}

// ErrInvalidCursor rejects a page token that does not decode, or whose key
// is not a valid position for the endpoint it was sent to.
type ErrInvalidCursor struct {
	Err error
}

func (e *ErrInvalidCursor) Error() string { return e.Err.Error() }
func (e *ErrInvalidCursor) Unwrap() error { return e.Err }

func (*ErrInvalidCursor) Kind() domain.ErrorKind { return domain.KindValidation }

var _ domain.Classifiable = (*ErrInvalidCursor)(nil)

// DecodeCursor decodes a page token, classifying a malformed one as a
// validation failure.
func DecodeCursor(token string) (pagecursor.Cursor, error) {
	c, err := pagecursor.Decode(token)
	if err != nil {
		return pagecursor.Cursor{}, &ErrInvalidCursor{Err: err}
	}

	return c, nil
}

// CursorUint64 parses the key of an endpoint keyed by a decimal id.
func CursorUint64(c pagecursor.Cursor) (uint64, error) {
	v, err := c.Uint64()
	if err != nil {
		return 0, &ErrInvalidCursor{Err: err}
	}

	return v, nil
}

// entityCursorKey renders a compiled-iterator entity as a cursor key: the
// decimal id for transactions and logs, the address for accounts.
func entityCursorKey(target ledgerpb.QueryTarget, entity []byte) string {
	if target == ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		return string(entity)
	}

	return strconv.FormatUint(binary.BigEndian.Uint64(entity), 10)
}

// entityFromCursorKey is the inverse of entityCursorKey. The empty key is no
// position.
func entityFromCursorKey(target ledgerpb.QueryTarget, c pagecursor.Cursor) ([]byte, error) {
	if c.Key == "" {
		return nil, nil
	}

	if target == ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS {
		return []byte(c.Key), nil
	}

	id, err := CursorUint64(c)
	if err != nil {
		return nil, fmt.Errorf("cursor for %v: %w", target, err)
	}

	return binary.BigEndian.AppendUint64(nil, id), nil
}
