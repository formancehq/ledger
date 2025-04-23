package common

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/platform/postgres"
	"github.com/formancehq/go-libs/v3/pointer"
	"github.com/formancehq/go-libs/v3/query"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/uptrace/bun"
	"math/big"
	"regexp"
	"slices"
)

func ConvertOperatorToSQL(operator string) string {
	switch operator {
	case "$match":
		return "="
	case "$lt":
		return "<"
	case "$gt":
		return ">"
	case "$lte":
		return "<="
	case "$gte":
		return ">="
	case "$like":
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

type Filter struct {
	Name       string
	Aliases    []string
	Matchers   []func(key string) bool
	Validators []PropertyValidator
}

type RepositoryHandlerBuildContext[Opts any] struct {
	ResourceQuery[Opts]
	filters map[string]any
}

func (ctx RepositoryHandlerBuildContext[Opts]) UseFilter(v string, matchers ...func(value any) bool) bool {
	value, ok := ctx.filters[v]
	if !ok {
		return false
	}
	for _, matcher := range matchers {
		if !matcher(value) {
			return false
		}
	}

	return true
}

type RepositoryHandler[Opts any] interface {
	Filters() []Filter
	BuildDataset(query RepositoryHandlerBuildContext[Opts]) (*bun.SelectQuery, error)
	ResolveFilter(query ResourceQuery[Opts], operator, property string, value any) (string, []any, error)
	Project(query ResourceQuery[Opts], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error)
	Expand(query ResourceQuery[Opts], property string) (*bun.SelectQuery, *JoinCondition, error)
}

type ResourceRepository[ResourceType, OptionsType any] struct {
	resourceHandler RepositoryHandler[OptionsType]
}

func (r *ResourceRepository[ResourceType, OptionsType]) validateFilters(builder query.Builder) (map[string]any, error) {
	if builder == nil {
		return nil, nil
	}

	ret := make(map[string]any)
	properties := r.resourceHandler.Filters()
	if err := builder.Walk(func(operator string, key string, value any) (err error) {

		found := false
		for _, property := range properties {
			if len(property.Matchers) > 0 {
				for _, matcher := range property.Matchers {
					if found = matcher(key); found {
						break
					}
				}
			} else {
				options := append([]string{property.Name}, property.Aliases...)
				for _, option := range options {
					if found, err = regexp.MatchString("^"+option+"$", key); err != nil {
						return fmt.Errorf("failed to match regex for key '%s': %w", key, err)
					} else if found {
						break
					}
				}
			}
			if !found {
				continue
			}

			for _, validator := range property.Validators {
				if err := validator.Validate(operator, key, value); err != nil {
					return err
				}
			}
			ret[property.Name] = value
			break
		}

		if !found {
			return NewErrInvalidQuery("unknown key '%s' when building query", key)
		}

		return nil
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

type PaginatedResourceRepository[ResourceType, OptionsType any, PaginationQueryType PaginatedQuery[OptionsType]] struct {
	*ResourceRepository[ResourceType, OptionsType]
	paginator Paginator[ResourceType, PaginationQueryType]
}

func (r *PaginatedResourceRepository[ResourceType, OptionsType, PaginationQueryType]) Paginate(
	ctx context.Context,
	paginationOptions PaginationQueryType,
) (*bunpaginate.Cursor[ResourceType], error) {

	var resourceQuery ResourceQuery[OptionsType]
	switch v := any(paginationOptions).(type) {
	case OffsetPaginatedQuery[OptionsType]:
		resourceQuery = v.Options
	case ColumnPaginatedQuery[OptionsType]:
		resourceQuery = v.Options
	default:
		panic("should not happen")
	}

	finalQuery, err := r.buildFilteredDataset(resourceQuery)
	if err != nil {
		return nil, fmt.Errorf("building filtered dataset: %w", err)
	}

	finalQuery, err = r.paginator.Paginate(finalQuery, paginationOptions)
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

	return r.paginator.BuildCursor(ret, paginationOptions)
}

func NewPaginatedResourceRepository[ResourceType, OptionsType any, PaginationQueryType PaginatedQuery[OptionsType]](
	handler RepositoryHandler[OptionsType],
	paginator Paginator[ResourceType, PaginationQueryType],
) *PaginatedResourceRepository[ResourceType, OptionsType, PaginationQueryType] {
	return &PaginatedResourceRepository[ResourceType, OptionsType, PaginationQueryType]{
		ResourceRepository: NewResourceRepository[ResourceType, OptionsType](handler),
		paginator:          paginator,
	}
}

type PaginatedResourceRepositoryMapper[ToResourceType any, OriginalResourceType interface {
	ToCore() ToResourceType
}, OptionsType any, PaginationQueryType PaginatedQuery[OptionsType]] struct {
	*PaginatedResourceRepository[OriginalResourceType, OptionsType, PaginationQueryType]
}

func (m PaginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType, PaginationQueryType]) Paginate(
	ctx context.Context,
	paginationOptions PaginationQueryType,
) (*bunpaginate.Cursor[ToResourceType], error) {
	cursor, err := m.PaginatedResourceRepository.Paginate(ctx, paginationOptions)
	if err != nil {
		return nil, err
	}

	return bunpaginate.MapCursor(cursor, OriginalResourceType.ToCore), nil
}

func (m PaginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType, PaginationQueryType]) GetOne(
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
}, OptionsType any, PaginationQueryType PaginatedQuery[OptionsType]](
	handler RepositoryHandler[OptionsType],
	paginator Paginator[OriginalResourceType, PaginationQueryType],
) *PaginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType, PaginationQueryType] {
	return &PaginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType, PaginationQueryType]{
		PaginatedResourceRepository: NewPaginatedResourceRepository(handler, paginator),
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

type (
	OffsetPaginatedQuery[OptionsType any] struct {
		Column   string                     `json:"column"`
		Offset   uint64                     `json:"offset"`
		Order    *bunpaginate.Order         `json:"order"`
		PageSize uint64                     `json:"pageSize"`
		Options  ResourceQuery[OptionsType] `json:"filters"`
	}
	ColumnPaginatedQuery[OptionsType any] struct {
		PageSize     uint64   `json:"pageSize"`
		Bottom       *big.Int `json:"bottom"`
		Column       string   `json:"column"`
		PaginationID *big.Int `json:"paginationID"`
		// todo: backport in go-libs
		Order   *bunpaginate.Order         `json:"order"`
		Options ResourceQuery[OptionsType] `json:"filters"`
		Reverse bool                       `json:"reverse"`
	}
	PaginatedQuery[OptionsType any] interface {
		OffsetPaginatedQuery[OptionsType] | ColumnPaginatedQuery[OptionsType]
	}
)

type PaginatedResource[ResourceType, OptionsType any, PaginationQueryType PaginatedQuery[OptionsType]] interface {
	Resource[ResourceType, OptionsType]
	Paginate(ctx context.Context, paginationOptions PaginationQueryType) (*bunpaginate.Cursor[ResourceType], error)
}
