package pagination

import (
	"context"

	"github.com/formancehq/ledger/pkg/storage"
	storageerrors "github.com/formancehq/ledger/pkg/storage/sqlstorage/errors"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/uptrace/bun"
)

type scanner[T any] func(t *T, scanner interface {
	Scan(args ...any) error
}) error

func UsingOffset[Q any, T any](ctx context.Context, sb *bun.SelectQuery, query storage.OffsetPaginatedQuery[Q], fn scanner[T]) (*api.Cursor[T], error) {
	ret := make([]T, 0)

	sb = sb.Offset(int(query.Offset))
	sb = sb.Limit(int(query.PageSize) + 1)

	rows, err := sb.Rows(ctx)
	if err != nil {
		return nil, storageerrors.PostgresError(err)
	}
	defer rows.Close()

	for rows.Next() {
		var t T
		if err := fn(&t, rows); err != nil {
			return nil, err
		}
		ret = append(ret, t)
	}

	if rows.Err() != nil {
		return nil, storageerrors.PostgresError(err)
	}

	var previous, next *storage.OffsetPaginatedQuery[Q]

	// Page with transactions before
	if query.Offset > 0 {
		cp := query
		offset := int(query.Offset) - int(query.PageSize)
		if offset < 0 {
			offset = 0
		}
		cp.Offset = uint64(offset)
		previous = &cp
	}

	// Page with transactions after
	if len(ret) > int(query.PageSize) {
		cp := query
		cp.Offset = query.Offset + query.PageSize
		next = &cp
		ret = ret[:len(ret)-1]
	}

	return &api.Cursor[T]{
		PageSize: int(query.PageSize),
		HasMore:  next != nil,
		Previous: previous.EncodeAsCursor(),
		Next:     next.EncodeAsCursor(),
		Data:     ret,
	}, nil
}
