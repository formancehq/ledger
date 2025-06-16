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
	"slices"
	"strings"
)

func ConvertOperatorToSQL(operator string) string {
	switch operator {
	case OperatorMatch:
		return "="
	case OperatorLT:
		return "<"
	case OperatorGT:
		return ">"
	case OperatorLTE:
		return "<="
	case OperatorGTE:
		return ">="
	case OperatorLike:
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

type EntitySchema struct {
	Fields map[string]Field
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
	Schema() EntitySchema
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
	properties := r.resourceHandler.Schema().Fields
	if err := builder.Walk(func(operator string, key string, value any) (err error) {

		for name, property := range properties {
			key := key
			if property.Type.IsIndexable() {
				key = strings.Split(key, "[")[0]
			}
			match := func() bool {
				if key == name {
					return true
				}
				for _, alias := range property.Aliases {
					if key == alias {
						return true
					}
				}

				return false
			}()
			if !match {
				continue
			}

			if !slices.Contains(property.Type.Operators(), operator) {
				return NewErrInvalidQuery("operator '%s' is not allowed for property '%s'", operator, name)
			}

			if err := property.Type.ValidateValue(value); err != nil {
				return NewErrInvalidQuery("invalid value '%v' for property '%s': %s", value, name, err)
			}

			ret[name] = value

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

const (
	OperatorMatch  = "$match"
	OperatorExists = "$exists"
	OperatorLike   = "$like"
	OperatorLT     = "$lt"
	OperatorGT     = "$gt"
	OperatorLTE    = "$lte"
	OperatorGTE    = "$gte"
)

type FieldType interface {
	Operators() []string
	ValidateValue(value any) error
	IsIndexable() bool
}

type Field struct {
	Aliases []string
	Type    FieldType
}

func (f Field) WithAliases(aliases ...string) Field {
	f.Aliases = append(f.Aliases, aliases...)
	return f
}

func NewField(t FieldType) Field {
	return Field{
		Aliases: []string{},
		Type:    t,
	}
}

// NewStringField creates a new field with TypeString as its type.
func NewStringField() Field {
	return NewField(NewTypeString())
}

// NewDateField creates a new field with TypeDate as its type.
func NewDateField() Field {
	return NewField(NewTypeDate())
}

// NewMapField creates a new field with TypeMap as its type, using the provided underlying type.
func NewMapField(underlyingType FieldType) Field {
	return NewField(NewTypeMap(underlyingType))
}

// NewNumericField creates a new field with TypeNumeric as its type.
func NewNumericField() Field {
	return NewField(NewTypeNumeric())
}

// NewBooleanField creates a new field with TypeBoolean as its type.
func NewBooleanField() Field {
	return NewField(NewTypeBoolean())
}

// NewStringMapField creates a new field with TypeMap as its type, using TypeString as the underlying type.
func NewStringMapField() Field {
	return NewMapField(NewTypeString())
}

// NewNumericMapField creates a new field with TypeMap as its type, using TypeNumeric as the underlying type.
func NewNumericMapField() Field {
	return NewMapField(NewTypeNumeric())
}

type TypeString struct{}

func (t TypeString) IsIndexable() bool {
	return false
}

func (t TypeString) Operators() []string {
	return []string{
		OperatorMatch,
		OperatorLike,
	}
}

func (t TypeString) ValidateValue(value any) error {
	_, ok := value.(string)
	if !ok {
		return fmt.Errorf("expected string value, got %T", value)
	}
	return nil
}

var _ FieldType = (*TypeString)(nil)

func NewTypeString() TypeString {
	return TypeString{}
}

type TypeDate struct{}

func (t TypeDate) IsIndexable() bool {
	return false
}

func (t TypeDate) Operators() []string {
	return []string{
		OperatorMatch,
		OperatorLT,
		OperatorGT,
		OperatorLTE,
		OperatorGTE,
	}
}

func (t TypeDate) ValidateValue(value any) error {
	switch value := value.(type) {
	case string:
		_, err := time.ParseTime(value)
		if err != nil {
			return fmt.Errorf("invalid date value: %w", err)
		}
	case time.Time, *time.Time:
	default:
		return fmt.Errorf("expected string, time.Time, or *time.Time value, got %T", value)
	}
	return nil
}

func NewTypeDate() TypeDate {
	return TypeDate{}
}

var _ FieldType = (*TypeDate)(nil)

type TypeMap struct {
	underlyingType FieldType
}

func (t TypeMap) IsIndexable() bool {
	return true
}

func (t TypeMap) Operators() []string {
	return append(t.underlyingType.Operators(), OperatorMatch, OperatorExists)
}

func (t TypeMap) ValidateValue(value any) error {
	return t.underlyingType.ValidateValue(value)
}

func NewTypeMap(underlyingType FieldType) TypeMap {
	return TypeMap{
		underlyingType: underlyingType,
	}
}

var _ FieldType = (*TypeMap)(nil)

type TypeNumeric struct{}

func (t TypeNumeric) IsIndexable() bool {
	return false
}

func (t TypeNumeric) Operators() []string {
	return []string{
		OperatorMatch,
		OperatorLT,
		OperatorGT,
		OperatorLTE,
		OperatorGTE,
	}
}

func (t TypeNumeric) ValidateValue(value any) error {
	switch value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float64, float32,
		*int, *int8, *int16, *int32, *int64, *uint, *uint8, *uint16, *uint32, *uint64, *float64, *float32:
		return nil
	default:
		return fmt.Errorf("expected numeric value, got %T", value)
	}
}

func NewTypeNumeric() TypeNumeric {
	return TypeNumeric{}
}

var _ FieldType = (*TypeNumeric)(nil)

type TypeBoolean struct{}

func (t TypeBoolean) IsIndexable() bool {
	return false
}

func (t TypeBoolean) Operators() []string {
	return []string{
		OperatorMatch,
	}
}

func (t TypeBoolean) ValidateValue(value any) error {
	_, ok := value.(bool)
	if !ok {
		return fmt.Errorf("expected boolean value, got %T", value)
	}

	return nil
}

func NewTypeBoolean() TypeBoolean {
	return TypeBoolean{}
}

var _ FieldType = (*TypeBoolean)(nil)
