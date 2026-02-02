package common

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"

	"github.com/formancehq/ledger/internal/queries"
)

func ConvertOperatorToSQL(operator string) string {
	switch operator {
	case queries.OperatorMatch:
		return "="
	case queries.OperatorLT:
		return "<"
	case queries.OperatorGT:
		return ">"
	case queries.OperatorLTE:
		return "<="
	case queries.OperatorGTE:
		return ">="
	case queries.OperatorLike:
		return "like"
	}
	panic("unreachable")
}

type JoinCondition struct {
	Left  string
	Right string
}

type PropertyValidator interface {
	Validate(string, string, any) error
}
type PropertyValidatorFunc func(string, string, any) error

func (p PropertyValidatorFunc) Validate(operator string, key string, value any) error {
	return p(operator, key, value)
}

func AcceptOperators(operators ...string) PropertyValidator {
	return PropertyValidatorFunc(func(operator string, key string, value any) error {
		if !slices.Contains(operators, operator) {
			return NewErrInvalidQuery("operator '%s' is not allowed", operator)
		}
		return nil
	})
}

type RepositoryHandlerBuildContext[Opts any] struct {
	ResourceQuery[Opts]
	filters map[string][]any
}

func (ctx RepositoryHandlerBuildContext[Opts]) UseFilter(v string, matchers ...func(value any) bool) bool {
	values, ok := ctx.filters[v]
	if !ok {
		return false
	}
	if len(matchers) == 0 {
		return true
	}
	// Return true if at least one value matches all matchers
	for _, value := range values {
		allMatch := true
		for _, matcher := range matchers {
			if !matcher(value) {
				allMatch = false
				break
			}
		}
		if allMatch {
			return true
		}
	}

	return false
}

type RepositoryHandler[Opts any] interface {
	Schema() queries.EntitySchema
	BuildDataset(query RepositoryHandlerBuildContext[Opts]) (*bun.SelectQuery, error)
	ResolveFilter(query ResourceQuery[Opts], operator, property string, value any) (string, []any, error)
	Project(query ResourceQuery[Opts], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error)
	Expand(query ResourceQuery[Opts], property string) (*bun.SelectQuery, *JoinCondition, error)
}

type ResourceRepository[ResourceType, OptionsType any] struct {
	resourceHandler RepositoryHandler[OptionsType]
}

func (r *ResourceRepository[ResourceType, OptionsType]) validateFilters(builder query.Builder) (map[string][]any, error) {
	if builder == nil {
		return nil, nil
	}

	ret := make(map[string][]any)
	properties := r.resourceHandler.Schema().Fields
	if err := builder.Walk(func(operator string, key string, value any) (err error) {
		for name, property := range properties {
			key := key
			if property.Type.IsIndexable() {
				key = strings.Split(key, "[")[0]
			}
			if !property.MatchKey(name, key) {
				continue
			}

			if !slices.Contains(property.Type.Operators(), operator) {
				return NewErrInvalidQuery("operator '%s' is not allowed for property '%s'", operator, name)
			}

			if err := property.Type.ValidateValue(operator, value); err != nil {
				return NewErrInvalidQuery("invalid value '%v' for property '%s': %s", value, name, err)
			}

			ret[name] = append(ret[name], value)

			return nil
		}

		return NewErrInvalidQuery("unknown key '%s' when building query", key)
	}); err != nil {
		return nil, err
	}

	return ret, nil
}

func (r *ResourceRepository[ResourceType, OptionsType]) buildFilteredDataset(q ResourceQuery[OptionsType]) (*bun.SelectQuery, error) {

	filters, err := r.validateFilters(q.Builder)
	if err != nil {
		return nil, err
	}

	dataset, err := r.resourceHandler.BuildDataset(RepositoryHandlerBuildContext[OptionsType]{
		ResourceQuery: q,
		filters:       filters,
	})
	if err != nil {
		return nil, err
	}

	dataset = dataset.NewSelect().
		ModelTableExpr("(?) dataset", dataset)

	if q.Builder != nil {
		// Convert filters to where clause
		where, args, err := q.Builder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			return r.resourceHandler.ResolveFilter(q, operator, key, value)
		}))
		if err != nil {
			return nil, err
		}
		if len(args) > 0 {
			dataset = dataset.Where(where, args...)
		} else {
			dataset = dataset.Where(where)
		}
	}

	return r.resourceHandler.Project(q, dataset)
}

func (r *ResourceRepository[ResourceType, OptionsType]) expand(dataset *bun.SelectQuery, q ResourceQuery[OptionsType]) (*bun.SelectQuery, error) {
	dataset = dataset.NewSelect().
		With("dataset", dataset).
		ModelTableExpr("dataset").
		ColumnExpr("*")

	slices.Sort(q.Expand)

	for i, expand := range q.Expand {
		selectQuery, joinCondition, err := r.resourceHandler.Expand(q, expand)
		if err != nil {
			return nil, err
		}

		if selectQuery == nil {
			continue
		}

		expandCTEName := fmt.Sprintf("expand%d", i)
		dataset = dataset.
			With(expandCTEName, selectQuery).
			Join(fmt.Sprintf(
				"left join %s on %s.%s = dataset.%s",
				expandCTEName,
				expandCTEName,
				joinCondition.Right,
				joinCondition.Left,
			))
	}

	return dataset, nil
}

func (r *ResourceRepository[ResourceType, OptionsType]) GetOne(ctx context.Context, query ResourceQuery[OptionsType]) (*ResourceType, error) {

	finalQuery, err := r.buildFilteredDataset(query)
	if err != nil {
		return nil, err
	}

	finalQuery, err = r.expand(finalQuery, query)
	if err != nil {
		return nil, err
	}

	ret := make([]ResourceType, 0)
	if err := finalQuery.
		Model(&ret).
		Limit(1).
		Scan(ctx); err != nil {
		return nil, err
	}
	if len(ret) == 0 {
		return nil, postgres.ErrNotFound
	}

	return &ret[0], nil
}

func (r *ResourceRepository[ResourceType, OptionsType]) Count(ctx context.Context, query ResourceQuery[OptionsType]) (int, error) {

	finalQuery, err := r.buildFilteredDataset(query)
	if err != nil {
		return 0, err
	}

	count, err := finalQuery.Count(ctx)
	return count, postgres.ResolveError(err)
}

func NewResourceRepository[ResourceType, OptionsType any](
	handler RepositoryHandler[OptionsType],
) *ResourceRepository[ResourceType, OptionsType] {
	return &ResourceRepository[ResourceType, OptionsType]{
		resourceHandler: handler,
	}
}

type PaginatedResourceRepository[ResourceType, OptionsType any] struct {
	defaultPaginationColumn string
	defaultOrder            bunpaginate.Order
	*ResourceRepository[ResourceType, OptionsType]
}

func (r *PaginatedResourceRepository[ResourceType, OptionsType]) Paginate(
	ctx context.Context,
	paginationQuery PaginatedQuery[OptionsType],
) (*bunpaginate.Cursor[ResourceType], error) {

	switch v := any(paginationQuery).(type) {
	case OffsetPaginatedQuery[OptionsType]:
	case ColumnPaginatedQuery[OptionsType]:
	case InitialPaginatedQuery[OptionsType]:

		if v.Column == "" {
			v.Column = r.defaultPaginationColumn
		}
		if v.Order == nil {
			v.Order = pointer.For(r.defaultOrder)
		}
		if v.PageSize == 0 {
			v.PageSize = bunpaginate.QueryDefaultPageSize
		}

		_, field := r.resourceHandler.Schema().GetFieldByNameOrAlias(v.Column)
		if field == nil {
			return nil, fmt.Errorf("invalid property '%s' for pagination", v.Column)
		}

		if !field.IsPaginated {
			return nil, newErrNotPaginatedField(v.Column)
		}

		if field.Type.IsPaginated() {
			paginationQuery = ColumnPaginatedQuery[OptionsType]{
				InitialPaginatedQuery: v,
			}
		} else {
			paginationQuery = OffsetPaginatedQuery[OptionsType]{
				InitialPaginatedQuery: v,
			}
		}
	default:
		panic(fmt.Errorf(
			"should not happen, got type when waiting for %T, %T, or %T: %T",
			InitialPaginatedQuery[OptionsType]{},
			OffsetPaginatedQuery[OptionsType]{},
			ColumnPaginatedQuery[OptionsType]{},
			paginationQuery,
		))
	}

	var (
		paginator     Paginator[ResourceType]
		resourceQuery ResourceQuery[OptionsType]
	)
	switch v := any(paginationQuery).(type) {
	case OffsetPaginatedQuery[OptionsType]:
		paginator = newOffsetPaginator[ResourceType, OptionsType](v)
		resourceQuery = v.Options
	case ColumnPaginatedQuery[OptionsType]:
		fieldName, field := r.resourceHandler.Schema().GetFieldByNameOrAlias(v.Column)
		if field == nil {
			return nil, fmt.Errorf("invalid property '%s' for pagination", v.Column)
		}
		paginator = newColumnPaginator[ResourceType, OptionsType](v, fieldName, field.Type)
		resourceQuery = v.Options
	default:
		panic("should not happen")
	}

	finalQuery, err := r.buildFilteredDataset(resourceQuery)
	if err != nil {
		return nil, fmt.Errorf("building filtered dataset: %w", err)
	}

	finalQuery, err = paginator.Paginate(finalQuery)
	if err != nil {
		return nil, fmt.Errorf("paginating request: %w", err)
	}

	finalQuery, err = r.expand(finalQuery, resourceQuery)
	if err != nil {
		return nil, fmt.Errorf("expanding results: %w", err)
	}
	finalQuery = finalQuery.Order("row_number")

	ret := make([]ResourceType, 0)
	err = finalQuery.Model(&ret).Scan(ctx)
	if err != nil {
		return nil, fmt.Errorf("scanning results: %w", err)
	}

	return paginator.BuildCursor(ret)
}

func NewPaginatedResourceRepository[ResourceType, OptionsType any](
	handler RepositoryHandler[OptionsType],
	defaultPaginationColumn string,
	defaultOrder bunpaginate.Order,
) *PaginatedResourceRepository[ResourceType, OptionsType] {
	return &PaginatedResourceRepository[ResourceType, OptionsType]{
		ResourceRepository:      NewResourceRepository[ResourceType, OptionsType](handler),
		defaultPaginationColumn: defaultPaginationColumn,
		defaultOrder:            defaultOrder,
	}
}

type PaginatedResourceRepositoryMapper[ToResourceType any, OriginalResourceType interface {
	ToCore() ToResourceType
}, OptionsType any] struct {
	*PaginatedResourceRepository[OriginalResourceType, OptionsType]
}

func (m PaginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType]) Paginate(
	ctx context.Context,
	paginatedQuery PaginatedQuery[OptionsType],
) (*bunpaginate.Cursor[ToResourceType], error) {
	cursor, err := m.PaginatedResourceRepository.Paginate(ctx, paginatedQuery)
	if err != nil {
		return nil, err
	}

	return bunpaginate.MapCursor(cursor, OriginalResourceType.ToCore), nil
}

func (m PaginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType]) GetOne(
	ctx context.Context,
	query ResourceQuery[OptionsType],
) (*ToResourceType, error) {
	item, err := m.PaginatedResourceRepository.GetOne(ctx, query)
	if err != nil {
		return nil, err
	}

	return pointer.For((*item).ToCore()), nil
}

func NewPaginatedResourceRepositoryMapper[ToResourceType any, OriginalResourceType interface {
	ToCore() ToResourceType
}, OptionsType any](
	handler RepositoryHandler[OptionsType],
	defaultPaginationColumn string,
	defaultOrder bunpaginate.Order,
) *PaginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType] {
	return &PaginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType]{
		PaginatedResourceRepository: NewPaginatedResourceRepository[OriginalResourceType, OptionsType](handler, defaultPaginationColumn, defaultOrder),
	}
}

type ResourceQuery[Opts any] struct {
	PIT     *time.Time    `json:"pit"`
	OOT     *time.Time    `json:"oot"`
	Builder query.Builder `json:"qb"`
	Expand  []string      `json:"expand,omitempty"`
	Opts    Opts          `json:"opts"`
}

func (rq ResourceQuery[Opts]) UsePIT() bool {
	return rq.PIT != nil && !rq.PIT.IsZero()
}

func (rq ResourceQuery[Opts]) UseOOT() bool {
	return rq.OOT != nil && !rq.OOT.IsZero()
}

func (rq *ResourceQuery[Opts]) UnmarshalJSON(data []byte) error {
	type rawResourceQuery ResourceQuery[Opts]
	type aux struct {
		rawResourceQuery
		Builder json.RawMessage `json:"qb"`
	}
	x := aux{}
	if err := json.Unmarshal(data, &x); err != nil {
		return err
	}

	var err error
	*rq = ResourceQuery[Opts](x.rawResourceQuery)
	rq.Builder, err = query.ParseJSON(string(x.Builder))

	return err
}

type Resource[ResourceType, OptionsType any] interface {
	GetOne(ctx context.Context, query ResourceQuery[OptionsType]) (*ResourceType, error)
	Count(ctx context.Context, query ResourceQuery[OptionsType]) (int, error)
}

type PaginatedResource[ResourceType, OptionsType any] interface {
	Resource[ResourceType, OptionsType]
	Paginate(ctx context.Context, paginationOptions PaginatedQuery[OptionsType]) (*bunpaginate.Cursor[ResourceType], error)
}
