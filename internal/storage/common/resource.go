package common

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/uptrace/bun"

	"github.com/formancehq/go-libs/v5/pkg/query"
	"github.com/formancehq/go-libs/v5/pkg/storage/bun/paginate"
	"github.com/formancehq/go-libs/v5/pkg/storage/postgres"
	"github.com/formancehq/go-libs/v5/pkg/types/pointer"
	"github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/internal/queries"
)

// NormalizeDateFilterValue parses a date filter value into a UTC time.
// Date columns are "timestamp without time zone" holding UTC instants;
// casting an offset-bearing string (e.g. 2026-05-21T15:09:13+04:00) to
// that type in Postgres silently drops the offset, so the value must be
// normalized to UTC application-side before being bound.
func NormalizeDateFilterValue(value any) (any, error) {
	if s, ok := value.(string); ok {
		ts, err := time.ParseTime(s)
		if err != nil {
			return nil, err
		}
		return ts, nil
	}
	return value, nil
}

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

// DatasetFencer is an optional interface a RepositoryHandler may implement to request a
// MATERIALIZED CTE fence around the filtered dataset when the resolved filter is selective
// enough that an abort-early "ORDER BY ... LIMIT" index walk would scan most of the table.
//
// When ShouldFenceDataset returns true, the paginated query is generated as:
//
//	WITH dataset AS MATERIALIZED (<filter + PIT + keyset>)
//	SELECT * FROM dataset [expand joins] ORDER BY dataset.<col> <dir> LIMIT pageSize+1
//
// i.e. the ORDER BY + LIMIT move outside the fence so the planner can pick a GIN BitmapOr
// over the filtered set instead of walking the pagination index. The decision is static
// (filter-shape based); see internal/storage/ledger/resource_transactions.go for the rule.
//
// Handlers that do not implement this interface keep the default, non-materialized shape.
type DatasetFencer[Opts any] interface {
	ShouldFenceDataset(ctx RepositoryHandlerBuildContext[Opts]) bool
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
	if err := builder.Walk(func(operator string, key string, value *any) (err error) {
		for name, property := range properties {
			key := key
			if property.Type.Index() != nil {
				key = strings.Split(key, "[")[0]
			}
			if !property.MatchKey(name, key) {
				continue
			}

			if !slices.Contains(property.Type.Operators(), operator) {
				return NewErrInvalidQuery("operator '%s' is not allowed for property '%s'", operator, name)
			}

			if err := property.Type.ValidateValue(operator, *value); err != nil {
				return NewErrInvalidQuery("invalid value '%v' for property '%s': %s", *value, name, err)
			}

			ret[name] = append(ret[name], *value)

			return nil
		}

		return NewErrInvalidQuery("unknown key '%s' when building query", key)
	}); err != nil {
		return nil, err
	}

	return ret, nil
}

// resolveBuildContext validates the query's filters and returns the build context shared by
// BuildDataset and the optional DatasetFencer decision. Kept separate from buildFilteredDataset
// so the paginated path can inspect the resolved filter shape before deciding whether to fence.
func (r *ResourceRepository[ResourceType, OptionsType]) resolveBuildContext(q ResourceQuery[OptionsType]) (RepositoryHandlerBuildContext[OptionsType], error) {
	filters, err := r.validateFilters(q.Builder)
	if err != nil {
		return RepositoryHandlerBuildContext[OptionsType]{}, err
	}

	return RepositoryHandlerBuildContext[OptionsType]{
		ResourceQuery: q,
		filters:       filters,
	}, nil
}

func (r *ResourceRepository[ResourceType, OptionsType]) buildFilteredDataset(buildCtx RepositoryHandlerBuildContext[OptionsType]) (*bun.SelectQuery, error) {

	q := buildCtx.ResourceQuery

	dataset, err := r.resourceHandler.BuildDataset(buildCtx)
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
		if where != "" {
			if len(args) > 0 {
				dataset = dataset.Where(where, args...)
			} else {
				dataset = dataset.Where(where)
			}
		}
	}

	return r.resourceHandler.Project(q, dataset)
}

// expand wraps the given select as the "dataset" CTE and left-joins any requested expand CTEs,
// which reference it via "select ... from dataset". The MATERIALIZED fence (when fencing) lives one
// level deeper, inside the select passed here (see Paginate), so the expands always join against the
// page rather than the full filtered set.
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

	buildCtx, err := r.resolveBuildContext(query)
	if err != nil {
		return nil, err
	}

	finalQuery, err := r.buildFilteredDataset(buildCtx)
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

	buildCtx, err := r.resolveBuildContext(query)
	if err != nil {
		return 0, err
	}

	finalQuery, err := r.buildFilteredDataset(buildCtx)
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
	defaultOrder            paginate.Order
	*ResourceRepository[ResourceType, OptionsType]
}

func (r *PaginatedResourceRepository[ResourceType, OptionsType]) Paginate(
	ctx context.Context,
	paginationQuery PaginatedQuery[OptionsType],
) (*paginate.Cursor[ResourceType], error) {

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
			v.PageSize = paginate.QueryDefaultPageSize
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

	buildCtx, err := r.resolveBuildContext(resourceQuery)
	if err != nil {
		return nil, fmt.Errorf("resolving build context: %w", err)
	}

	// A handler may opt into a MATERIALIZED CTE fence when its resolved filter is selective.
	// Fenced, the keyset predicate stays inside the CTE and ORDER BY + LIMIT move to the outer
	// select; unfenced keeps the historical shape (order + limit inside the dataset select).
	fence := false
	if fencer, ok := r.resourceHandler.(DatasetFencer[OptionsType]); ok {
		fence = fencer.ShouldFenceDataset(buildCtx)
	}

	finalQuery, err := r.buildFilteredDataset(buildCtx)
	if err != nil {
		return nil, fmt.Errorf("building filtered dataset: %w", err)
	}

	if fence {
		// The cursor/offset predicate is part of "the filtered set", so it stays inside the fence.
		finalQuery = paginator.ApplyCursorPredicate(finalQuery)

		// Fence the filtered set in a MATERIALIZED CTE ("filtered"), then take the page
		// (ORDER BY + LIMIT/OFFSET) in the "dataset" select that wraps it. The page window lives
		// here — between the fence and the expand joins — so expand CTEs that reference
		// "select id from dataset" see only the page, not the whole filtered set. The plan benefit
		// is preserved: the LIMIT is outside the *materialized* CTE, so the planner still evaluates
		// "filtered" once via the GIN BitmapOr and the page is a cheap top-N over that result.
		paged := finalQuery.NewSelect().
			WithQuery(bun.NewWithQuery("filtered", finalQuery).Materialized()).
			ModelTableExpr("filtered").
			ColumnExpr("*").
			Order(paginator.OrderExpression())
		paged, err = paginator.ApplyWindow(paged)
		if err != nil {
			return nil, fmt.Errorf("applying page window: %w", err)
		}

		// expand wraps the paged select as the "dataset" CTE the expands join against.
		finalQuery, err = r.expand(paged, resourceQuery)
		if err != nil {
			return nil, fmt.Errorf("expanding results: %w", err)
		}
	} else {
		finalQuery, err = paginator.Paginate(finalQuery)
		if err != nil {
			return nil, fmt.Errorf("paginating request: %w", err)
		}
		finalQuery, err = r.expand(finalQuery, resourceQuery)
		if err != nil {
			return nil, fmt.Errorf("expanding results: %w", err)
		}
	}
	// Qualify the sort column with "dataset." so that LEFT JOINed expand CTEs
	// cannot introduce an ambiguous column name (e.g. a future expand that also
	// selects "id"). No expand today conflicts, but this makes it safe by construction.
	// This is the final ORDER BY over the joined result for both paths; in the fenced path the
	// page LIMIT already lives inside the "dataset" CTE, so the outer select carries no LIMIT.
	orderExpr := paginator.OrderExpression()
	col, dir, _ := strings.Cut(orderExpr, " ")
	finalQuery = finalQuery.Order(fmt.Sprintf("dataset.%s %s", col, dir))

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
	defaultOrder paginate.Order,
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
) (*paginate.Cursor[ToResourceType], error) {
	cursor, err := m.PaginatedResourceRepository.Paginate(ctx, paginatedQuery)
	if err != nil {
		return nil, err
	}

	return paginate.MapCursor(cursor, OriginalResourceType.ToCore), nil
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
	defaultOrder paginate.Order,
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
	Paginate(ctx context.Context, paginationOptions PaginatedQuery[OptionsType]) (*paginate.Cursor[ResourceType], error)
}
