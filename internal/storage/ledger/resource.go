package ledger

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/platform/postgres"
	"github.com/formancehq/go-libs/v2/pointer"
	"github.com/formancehq/go-libs/v2/query"
	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/uptrace/bun"
	"regexp"
	"slices"
)

func convertOperatorToSQL(operator string) string {
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
	}
	panic("unreachable")
}

type joinCondition struct {
	left  string
	right string
}

type propertyValidator interface {
	validate(ledger.Ledger, string, string, any) error
}
type propertyValidatorFunc func(ledger.Ledger, string, string, any) error

func (p propertyValidatorFunc) validate(l ledger.Ledger, operator string, key string, value any) error {
	return p(l, operator, key, value)
}

func acceptOperators(operators ...string) propertyValidator {
	return propertyValidatorFunc(func(l ledger.Ledger, operator string, key string, value any) error {
		if !slices.Contains(operators, operator) {
			return ledgercontroller.NewErrInvalidQuery("operator '%s' is not allowed", operator)
		}
		return nil
	})
}

type filter struct {
	name       string
	aliases    []string
	matchers   []func(key string) bool
	validators []propertyValidator
}

type repositoryHandlerBuildContext[Opts any] struct {
	ledgercontroller.ResourceQuery[Opts]
	filters map[string]any
}

func (ctx repositoryHandlerBuildContext[Opts]) useFilter(v string, matchers ...func(value any) bool) bool {
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

type repositoryHandler[Opts any] interface {
	filters() []filter
	buildDataset(store *Store, query repositoryHandlerBuildContext[Opts]) (*bun.SelectQuery, error)
	resolveFilter(store *Store, query ledgercontroller.ResourceQuery[Opts], operator, property string, value any) (string, []any, error)
	project(store *Store, query ledgercontroller.ResourceQuery[Opts], selectQuery *bun.SelectQuery) (*bun.SelectQuery, error)
	expand(store *Store, query ledgercontroller.ResourceQuery[Opts], property string) (*bun.SelectQuery, *joinCondition, error)
}

type resourceRepository[ResourceType, OptionsType any] struct {
	resourceHandler repositoryHandler[OptionsType]
	store           *Store
	ledger          ledger.Ledger
}

func (r *resourceRepository[ResourceType, OptionsType]) validateFilters(builder query.Builder) (map[string]any, error) {
	if builder == nil {
		return nil, nil
	}

	ret := make(map[string]any)
	properties := r.resourceHandler.filters()
	if err := builder.Walk(func(operator string, key string, value any) (err error) {

		found := false
		for _, property := range properties {
			if len(property.matchers) > 0 {
				for _, matcher := range property.matchers {
					if found = matcher(key); found {
						break
					}
				}
			} else {
				options := append([]string{property.name}, property.aliases...)
				for _, option := range options {
					if found, err = regexp.MatchString("^"+option+"$", key); err != nil {
						panic(err)
					} else if found {
						break
					}
				}
			}
			if !found {
				continue
			}

			for _, validator := range property.validators {
				if err := validator.validate(r.ledger, operator, key, value); err != nil {
					return err
				}
			}
			ret[property.name] = value
			break
		}

		if !found {
			return ledgercontroller.NewErrInvalidQuery("unknown key '%s' when building query", key)
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return ret, nil
}

func (r *resourceRepository[ResourceType, OptionsType]) buildFilteredDataset(q ledgercontroller.ResourceQuery[OptionsType]) (*bun.SelectQuery, error) {

	filters, err := r.validateFilters(q.Builder)
	if err != nil {
		return nil, err
	}

	dataset, err := r.resourceHandler.buildDataset(r.store, repositoryHandlerBuildContext[OptionsType]{
		ResourceQuery: q,
		filters:       filters,
	})
	if err != nil {
		return nil, err
	}

	dataset = r.store.db.NewSelect().
		ModelTableExpr("(?) dataset", dataset)

	if q.Builder != nil {
		// Convert filters to where clause
		where, args, err := q.Builder.Build(query.ContextFn(func(key, operator string, value any) (string, []any, error) {
			return r.resourceHandler.resolveFilter(r.store, q, operator, key, value)
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

	return r.resourceHandler.project(r.store, q, dataset)
}

func (r *resourceRepository[ResourceType, OptionsType]) expand(dataset *bun.SelectQuery, q ledgercontroller.ResourceQuery[OptionsType]) (*bun.SelectQuery, error) {
	dataset = r.store.db.NewSelect().
		With("dataset", dataset).
		ModelTableExpr("dataset").
		ColumnExpr("*")

	slices.Sort(q.Expand)

	for i, expand := range q.Expand {
		selectQuery, joinCondition, err := r.resourceHandler.expand(r.store, q, expand)
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
				joinCondition.right,
				joinCondition.left,
			))
	}

	return dataset, nil
}

func (r *resourceRepository[ResourceType, OptionsType]) GetOne(ctx context.Context, query ledgercontroller.ResourceQuery[OptionsType]) (*ResourceType, error) {

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

func (r *resourceRepository[ResourceType, OptionsType]) Count(ctx context.Context, query ledgercontroller.ResourceQuery[OptionsType]) (int, error) {

	finalQuery, err := r.buildFilteredDataset(query)
	if err != nil {
		return 0, err
	}

	count, err := finalQuery.Count(ctx)
	return count, postgres.ResolveError(err)
}

func newResourceRepository[ResourceType, OptionsType any](
	store *Store,
	l ledger.Ledger,
	handler repositoryHandler[OptionsType],
) *resourceRepository[ResourceType, OptionsType] {
	return &resourceRepository[ResourceType, OptionsType]{
		resourceHandler: handler,
		store:           store,
		ledger:          l,
	}
}

type paginatedResourceRepository[ResourceType, OptionsType any, PaginationQueryType ledgercontroller.PaginatedQuery[OptionsType]] struct {
	*resourceRepository[ResourceType, OptionsType]
	paginator paginator[ResourceType, PaginationQueryType]
}

func (r *paginatedResourceRepository[ResourceType, OptionsType, PaginationQueryType]) Paginate(
	ctx context.Context,
	paginationOptions PaginationQueryType,
) (*bunpaginate.Cursor[ResourceType], error) {

	var resourceQuery ledgercontroller.ResourceQuery[OptionsType]
	switch v := any(paginationOptions).(type) {
	case ledgercontroller.OffsetPaginatedQuery[OptionsType]:
		resourceQuery = v.Options
	case ledgercontroller.ColumnPaginatedQuery[OptionsType]:
		resourceQuery = v.Options
	default:
		panic("should not happen")
	}

	finalQuery, err := r.buildFilteredDataset(resourceQuery)
	if err != nil {
		return nil, err
	}

	finalQuery = r.paginator.paginate(finalQuery, paginationOptions)

	finalQuery, err = r.expand(finalQuery, resourceQuery)
	if err != nil {
		return nil, err
	}
	finalQuery = finalQuery.Order("row_number")

	ret := make([]ResourceType, 0)
	//fmt.Println(finalQuery.Model(&ret).String())
	err = finalQuery.Model(&ret).Scan(ctx)
	if err != nil {
		return nil, err
	}

	return r.paginator.buildCursor(ret, paginationOptions)
}

func newPaginatedResourceRepository[ResourceType, OptionsType any, PaginationQueryType ledgercontroller.PaginatedQuery[OptionsType]](
	store *Store,
	l ledger.Ledger,
	handler repositoryHandler[OptionsType],
	paginator paginator[ResourceType, PaginationQueryType],
) *paginatedResourceRepository[ResourceType, OptionsType, PaginationQueryType] {
	return &paginatedResourceRepository[ResourceType, OptionsType, PaginationQueryType]{
		resourceRepository: newResourceRepository[ResourceType, OptionsType](store, l, handler),
		paginator:          paginator,
	}
}

type paginatedResourceRepositoryMapper[ToResourceType any, OriginalResourceType interface {
	ToCore() ToResourceType
}, OptionsType any, PaginationQueryType ledgercontroller.PaginatedQuery[OptionsType]] struct {
	*paginatedResourceRepository[OriginalResourceType, OptionsType, PaginationQueryType]
}

func (m paginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType, PaginationQueryType]) Paginate(
	ctx context.Context,
	paginationOptions PaginationQueryType,
) (*bunpaginate.Cursor[ToResourceType], error) {
	cursor, err := m.paginatedResourceRepository.Paginate(ctx, paginationOptions)
	if err != nil {
		return nil, err
	}

	return bunpaginate.MapCursor(cursor, OriginalResourceType.ToCore), nil
}

func (m paginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType, PaginationQueryType]) GetOne(
	ctx context.Context,
	query ledgercontroller.ResourceQuery[OptionsType],
) (*ToResourceType, error) {
	item, err := m.paginatedResourceRepository.GetOne(ctx, query)
	if err != nil {
		return nil, err
	}

	return pointer.For((*item).ToCore()), nil
}

func newPaginatedResourceRepositoryMapper[ToResourceType any, OriginalResourceType interface {
	ToCore() ToResourceType
}, OptionsType any, PaginationQueryType ledgercontroller.PaginatedQuery[OptionsType]](
	store *Store,
	l ledger.Ledger,
	handler repositoryHandler[OptionsType],
	paginator paginator[OriginalResourceType, PaginationQueryType],
) *paginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType, PaginationQueryType] {
	return &paginatedResourceRepositoryMapper[ToResourceType, OriginalResourceType, OptionsType, PaginationQueryType]{
		paginatedResourceRepository: newPaginatedResourceRepository(store, l, handler, paginator),
	}
}
