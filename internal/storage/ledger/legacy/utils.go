package legacy

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"reflect"
	"strings"

	"github.com/formancehq/go-libs/v2/time"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"

	"github.com/uptrace/bun"
)

func fetch[T any](s *Store, addModel bool, ctx context.Context, builders ...func(query *bun.SelectQuery) *bun.SelectQuery) (T, error) {

	var ret T
	ret = reflect.New(reflect.TypeOf(ret).Elem()).Interface().(T)

	query := s.db.NewSelect()

	if addModel {
		query = query.Model(ret)
	}

	for _, builder := range builders {
		query = query.Apply(builder)
	}

	if err := query.Scan(ctx, ret); err != nil {
		return ret, postgres.ResolveError(err)
	}

	return ret, nil
}

func paginateWithOffset[FILTERS any, RETURN any](s *Store, ctx context.Context,
	q *bunpaginate.OffsetPaginatedQuery[FILTERS], builders ...func(query *bun.SelectQuery) *bun.SelectQuery) (*bunpaginate.Cursor[RETURN], error) {

	query := s.db.NewSelect()
	for _, builder := range builders {
		query = query.Apply(builder)
	}
	return bunpaginate.UsingOffset[FILTERS, RETURN](ctx, query, *q)
}

func paginateWithOffsetWithoutModel[FILTERS any, RETURN any](s *Store, ctx context.Context,
	q *bunpaginate.OffsetPaginatedQuery[FILTERS], builders ...func(query *bun.SelectQuery) *bun.SelectQuery) (*bunpaginate.Cursor[RETURN], error) {

	query := s.db.NewSelect()
	for _, builder := range builders {
		query = query.Apply(builder)
	}

	return bunpaginate.UsingOffset[FILTERS, RETURN](ctx, query, *q)
}

func paginateWithColumn[FILTERS any, RETURN any](s *Store, ctx context.Context, q *bunpaginate.ColumnPaginatedQuery[FILTERS], builders ...func(query *bun.SelectQuery) *bun.SelectQuery) (*bunpaginate.Cursor[RETURN], error) {
	query := s.db.NewSelect()
	for _, builder := range builders {
		query = query.Apply(builder)
	}

	ret, err := bunpaginate.UsingColumn[FILTERS, RETURN](ctx, query, *q)
	if err != nil {
		return nil, postgres.ResolveError(err)
	}

	return ret, nil
}

func count[T any](s *Store, addModel bool, ctx context.Context, builders ...func(query *bun.SelectQuery) *bun.SelectQuery) (int, error) {
	query := s.db.NewSelect()
	if addModel {
		query = query.Model((*T)(nil))
	}
	for _, builder := range builders {
		query = query.Apply(builder)
	}
	return s.db.NewSelect().
		TableExpr("(" + query.String() + ") data").
		Count(ctx)
}

func filterAccountAddress(address, key string) string {
	parts := make([]string, 0)
	src := strings.Split(address, ":")

	needSegmentCheck := false
	for _, segment := range src {
		needSegmentCheck = segment == ""
		if needSegmentCheck {
			break
		}
	}

	if needSegmentCheck {
		parts = append(parts, fmt.Sprintf("jsonb_array_length(%s_array) = %d", key, len(src)))

		for i, segment := range src {
			if len(segment) == 0 {
				continue
			}
			parts = append(parts, fmt.Sprintf("%s_array @@ ('$[%d] == \"%s\"')::jsonpath", key, i, segment))
		}
	} else {
		parts = append(parts, fmt.Sprintf("%s = '%s'", key, address))
	}

	return strings.Join(parts, " and ")
}

func filterAccountAddressOnTransactions(address string, source, destination bool) string {
	src := strings.Split(address, ":")

	needSegmentCheck := false
	for _, segment := range src {
		needSegmentCheck = segment == ""
		if needSegmentCheck {
			break
		}
	}

	if needSegmentCheck {
		m := map[string]any{
			fmt.Sprint(len(src)): nil,
		}
		parts := make([]string, 0)

		for i, segment := range src {
			if len(segment) == 0 {
				continue
			}
			m[fmt.Sprint(i)] = segment
		}

		data, err := json.Marshal([]any{m})
		if err != nil {
			panic(err)
		}

		if source {
			parts = append(parts, fmt.Sprintf("sources_arrays @> '%s'", string(data)))
		}
		if destination {
			parts = append(parts, fmt.Sprintf("destinations_arrays @> '%s'", string(data)))
		}
		return strings.Join(parts, " or ")
	} else {
		data, err := json.Marshal([]string{address})
		if err != nil {
			panic(err)
		}

		parts := make([]string, 0)
		if source {
			parts = append(parts, fmt.Sprintf("sources @> '%s'", string(data)))
		}
		if destination {
			parts = append(parts, fmt.Sprintf("destinations @> '%s'", string(data)))
		}
		return strings.Join(parts, " or ")
	}
}

func filterPIT(pit *time.Time, column string) func(query *bun.SelectQuery) *bun.SelectQuery {
	return func(query *bun.SelectQuery) *bun.SelectQuery {
		if pit == nil || pit.IsZero() {
			return query
		}
		return query.Where(fmt.Sprintf("%s <= ?", column), pit)
	}
}

func filterOOT(oot *time.Time, column string) func(query *bun.SelectQuery) *bun.SelectQuery {
	return func(query *bun.SelectQuery) *bun.SelectQuery {
		if oot == nil || oot.IsZero() {
			return query
		}
		return query.Where(fmt.Sprintf("%s >= ?", column), oot)
	}
}
