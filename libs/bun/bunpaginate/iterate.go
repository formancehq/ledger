package bunpaginate

import (
	"context"
	"reflect"

	sharedapi "github.com/formancehq/stack/libs/go-libs/api"
	"github.com/pkg/errors"
)

func Iterate[T any, Q any](ctx context.Context, q Q, iterator func(ctx context.Context, q Q) (*sharedapi.Cursor[T], error), cb func(cursor *sharedapi.Cursor[T]) error) error {

	for {
		cursor, err := iterator(ctx, q)
		if err != nil {
			return err
		}

		if err := cb(cursor); err != nil {
			return err
		}

		if !cursor.HasMore {
			break
		}

		newQuery := reflect.New(reflect.TypeOf(q))
		if err := UnmarshalCursor(cursor.Next, newQuery.Interface()); err != nil {
			return errors.Wrap(err, "paginating next request")
		}

		q = newQuery.Elem().Interface().(Q)
	}

	return nil
}
