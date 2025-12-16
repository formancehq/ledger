package legacy

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/pointer"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	ledgerstore "github.com/formancehq/ledger/internal/storage/ledger"

	"math/big"
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

func paginateWithOffset[FILTERS any, RETURN any](
	s *Store,
	ctx context.Context,
	q *bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]],
	filters FILTERS,
	builders ...func(query *bun.SelectQuery) *bun.SelectQuery,
) (*bunpaginate.Cursor[RETURN], error) {

	query := s.db.NewSelect()
	for _, builder := range builders {
		query = query.Apply(builder)
	}
	return UsingOffset[FILTERS, RETURN](ctx, query, *q, filters)
}

func paginateWithOffsetWithoutModel[FILTERS any, RETURN any](
	s *Store,
	ctx context.Context,
	q *bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]],
	filters FILTERS,
	builders ...func(query *bun.SelectQuery) *bun.SelectQuery,
) (*bunpaginate.Cursor[RETURN], error) {

	query := s.db.NewSelect()
	for _, builder := range builders {
		query = query.Apply(builder)
	}

	return UsingOffset[FILTERS, RETURN](ctx, query, *q, filters)
}

func paginateWithColumn[FILTERS any, RETURN any](
	s *Store,
	ctx context.Context,
	q *bunpaginate.ColumnPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]],
	filters FILTERS,
	builders ...func(query *bun.SelectQuery) *bun.SelectQuery,
) (*bunpaginate.Cursor[RETURN], error) {
	query := s.db.NewSelect()
	for _, builder := range builders {
		query = query.Apply(builder)
	}

	ret, err := UsingColumn[FILTERS, RETURN](ctx, query, *q, filters)
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

func convertColumnCursor[FILTERS any](cp *bunpaginate.ColumnPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]], filters FILTERS) *ledgercontroller.ColumnPaginatedQuery[any] {

	var (
		pitFilterWithVolumes *PITFilterWithVolumes
	)
	switch (any)(filters).(type) {
	case PITFilterWithVolumes:
		pitFilterWithVolumes = pointer.For((any)(filters).(PITFilterWithVolumes))
	}

	return &ledgercontroller.ColumnPaginatedQuery[any]{
		PageSize:     cp.PageSize,
		Bottom:       cp.Bottom,
		Column:       cp.Column,
		PaginationID: cp.PaginationID,
		Order:        pointer.For(cp.Order),
		Options: ledgercontroller.ResourceQuery[any]{
			PIT: func() *time.Time {
				if pitFilterWithVolumes != nil {
					return pitFilterWithVolumes.PIT
				}
				return nil
			}(),
			OOT: func() *time.Time {
				if pitFilterWithVolumes != nil {
					return pitFilterWithVolumes.OOT
				}
				return nil
			}(),
			Builder: cp.Options.QueryBuilder,
			Expand: func() []string {
				ret := []string{}
				if pitFilterWithVolumes != nil && pitFilterWithVolumes.ExpandVolumes {
					ret = append(ret, "filters")
				}
				if pitFilterWithVolumes != nil && pitFilterWithVolumes.ExpandEffectiveVolumes {
					ret = append(ret, "effectiveVolumes")
				}
				return ret
			}(),
			//Opts: filters,
		},
		Reverse: cp.Reverse,
	}
}

func convertOffsetCursor[FILTERS any](cp *bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]], filters FILTERS) *ledgercontroller.OffsetPaginatedQuery[any] {

	var (
		pitFilterWithVolumes *PITFilterWithVolumes
		filtersForVolumes    *FiltersForVolumes
	)
	switch (any)(filters).(type) {
	case PITFilterWithVolumes:
		pitFilterWithVolumes = pointer.For((any)(filters).(PITFilterWithVolumes))
	case FiltersForVolumes:
		filtersForVolumes = pointer.For((any)(filters).(FiltersForVolumes))
	}

	return &ledgercontroller.OffsetPaginatedQuery[any]{
		Offset:   cp.Offset,
		Order:    pointer.For(cp.Order),
		PageSize: cp.PageSize,
		Options: ledgercontroller.ResourceQuery[any]{
			PIT: func() *time.Time {
				if pitFilterWithVolumes != nil {
					return pitFilterWithVolumes.PIT
				}
				if filtersForVolumes != nil {
					return filtersForVolumes.PIT
				}
				return nil
			}(),
			OOT: func() *time.Time {
				if pitFilterWithVolumes != nil {
					return pitFilterWithVolumes.OOT
				}
				if filtersForVolumes != nil {
					return filtersForVolumes.OOT
				}
				return nil
			}(),
			Builder: cp.Options.QueryBuilder,
			Expand: func() []string {
				ret := []string{}
				if pitFilterWithVolumes != nil && pitFilterWithVolumes.ExpandVolumes {
					ret = append(ret, "filters")
				}
				if pitFilterWithVolumes != nil && pitFilterWithVolumes.ExpandEffectiveVolumes {
					ret = append(ret, "effectiveVolumes")
				}
				return ret
			}(),
			Opts: func() any {
				if filtersForVolumes != nil {
					return filtersForVolumes
				}
				return nil
			}(),
		},
	}
}

func UsingColumn[FILTERS, ENTITY any](ctx context.Context,
	sb *bun.SelectQuery,
	query bunpaginate.ColumnPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]],
	filters FILTERS,
) (*bunpaginate.Cursor[ENTITY], error) {
	ret := make([]ENTITY, 0)

	sb = sb.Model(&ret)
	sb = sb.Limit(int(query.PageSize) + 1) // Fetch one additional item to find the next token
	order := query.Order
	if query.Reverse {
		order = order.Reverse()
	}
	sb = sb.OrderExpr(fmt.Sprintf("%s %s", query.Column, order))

	if query.PaginationID != nil {
		if query.Reverse {
			switch query.Order {
			case bunpaginate.OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s < ?", query.Column), query.PaginationID)
			case bunpaginate.OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s > ?", query.Column), query.PaginationID)
			}
		} else {
			switch query.Order {
			case bunpaginate.OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s >= ?", query.Column), query.PaginationID)
			case bunpaginate.OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s <= ?", query.Column), query.PaginationID)
			}
		}
	}

	if err := sb.Scan(ctx); err != nil {
		return nil, err
	}

	var v ENTITY
	fields := ledgerstore.FindPaginationFieldPath(v, query.Column)

	var (
		paginationIDs = make([]*big.Int, 0)
	)
	for _, t := range ret {
		paginationID := ledgerstore.FindPaginationField(t, fields...)
		if query.Bottom == nil {
			query.Bottom = paginationID
		}
		paginationIDs = append(paginationIDs, paginationID)
	}

	hasMore := len(ret) > int(query.PageSize)
	if hasMore {
		ret = ret[:len(ret)-1]
	}
	if query.Reverse {
		for i := 0; i < len(ret)/2; i++ {
			ret[i], ret[len(ret)-i-1] = ret[len(ret)-i-1], ret[i]
		}
	}

	var previous, next *bunpaginate.ColumnPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]]

	if query.Reverse {
		cp := query
		cp.Reverse = false
		next = &cp

		if hasMore {
			cp := query
			cp.PaginationID = paginationIDs[len(paginationIDs)-2]
			previous = &cp
		}
	} else {
		if hasMore {
			cp := query
			cp.PaginationID = paginationIDs[len(paginationIDs)-1]
			next = &cp
		}
		if query.PaginationID != nil {
			if (query.Order == bunpaginate.OrderAsc && query.PaginationID.Cmp(query.Bottom) > 0) || (query.Order == bunpaginate.OrderDesc && query.PaginationID.Cmp(query.Bottom) < 0) {
				cp := query
				cp.Reverse = true
				previous = &cp
			}
		}
	}
	var (
		nextConverted     *ledgercontroller.ColumnPaginatedQuery[any]
		previousConverted *ledgercontroller.ColumnPaginatedQuery[any]
	)
	if next != nil {
		nextConverted = convertColumnCursor[FILTERS](next, filters)
	}
	if previous != nil {
		previousConverted = convertColumnCursor[FILTERS](previous, filters)
	}

	return &bunpaginate.Cursor[ENTITY]{
		PageSize: int(query.PageSize),
		HasMore:  next != nil,
		Previous: ledgerstore.EncodeCursor[any, ledgercontroller.ColumnPaginatedQuery[any]](previousConverted),
		Next:     ledgerstore.EncodeCursor[any, ledgercontroller.ColumnPaginatedQuery[any]](nextConverted),
		Data:     ret,
	}, nil
}

func usingOffset[FILTERS any, ENTITY any](
	ctx context.Context,
	sb *bun.SelectQuery,
	query bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]],
	filters FILTERS,
	builders ...func(query *bun.SelectQuery) *bun.SelectQuery,
) (*bunpaginate.Cursor[ENTITY], error) {

	ret := make([]ENTITY, 0)

	sb.Model(&ret)
	for _, builder := range builders {
		sb = sb.Apply(builder)
	}

	if query.Offset > 0 {
		sb = sb.Offset(int(query.Offset))
	}

	if query.PageSize > 0 {
		sb = sb.Limit(int(query.PageSize) + 1)
	}

	if err := sb.Scan(ctx); err != nil {
		return nil, err
	}

	var previous, next *bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]]

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
	if query.PageSize != 0 && len(ret) > int(query.PageSize) {
		cp := query
		cp.Offset = query.Offset + query.PageSize
		next = &cp
		ret = ret[:len(ret)-1]
	}

	var (
		nextConverted     *ledgercontroller.OffsetPaginatedQuery[any]
		previousConverted *ledgercontroller.OffsetPaginatedQuery[any]
	)
	if next != nil {
		nextConverted = convertOffsetCursor[FILTERS](next, filters)
	}
	if previous != nil {
		previousConverted = convertOffsetCursor[FILTERS](previous, filters)
	}

	return &bunpaginate.Cursor[ENTITY]{
		PageSize: int(query.PageSize),
		HasMore:  next != nil,
		Previous: ledgerstore.EncodeCursor[any, ledgercontroller.OffsetPaginatedQuery[any]](previousConverted),
		Next:     ledgerstore.EncodeCursor[any, ledgercontroller.OffsetPaginatedQuery[any]](nextConverted),
		Data:     ret,
	}, nil
}

func UsingOffset[FILTERS any, ENTITY any](
	ctx context.Context,
	sb *bun.SelectQuery,
	query bunpaginate.OffsetPaginatedQuery[ledgercontroller.PaginatedQueryOptions[FILTERS]],
	filters FILTERS,
	builders ...func(query *bun.SelectQuery) *bun.SelectQuery,
) (*bunpaginate.Cursor[ENTITY], error) {
	return usingOffset[FILTERS, ENTITY](ctx, sb, query, filters, builders...)
}
