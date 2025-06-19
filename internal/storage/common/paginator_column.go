package common

import (
	"fmt"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/time"
	"github.com/uptrace/bun"
	"math/big"
	"reflect"
	"strings"
	libtime "time"
)

type columnPaginator[ResourceType, OptionsType any] struct {
	fieldType FieldType
	fieldName string
	query     ColumnPaginatedQuery[OptionsType]
}

//nolint:unused
func (o columnPaginator[ResourceType, OptionsType]) Paginate(sb *bun.SelectQuery) (*bun.SelectQuery, error) {

	paginationColumn := o.fieldName
	originalOrder := *o.query.Order

	pageSize := o.query.PageSize
	if pageSize == 0 {
		pageSize = bunpaginate.QueryDefaultPageSize
	}

	sb = sb.Limit(int(pageSize) + 1) // Fetch one additional item to find the next token

	order := originalOrder
	if o.query.Reverse {
		order = order.Reverse()
	}
	orderExpression := fmt.Sprintf("%s %s", paginationColumn, order)
	sb = sb.ColumnExpr("row_number() OVER (ORDER BY " + orderExpression + ")")

	if o.query.PaginationID != nil {
		paginationID := convertPaginationIDToSQLType(o.fieldType, o.query.PaginationID)
		if o.query.Reverse {
			switch originalOrder {
			case bunpaginate.OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s < ?", paginationColumn), paginationID)
			case bunpaginate.OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s > ?", paginationColumn), paginationID)
			}
		} else {
			switch originalOrder {
			case bunpaginate.OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s >= ?", paginationColumn), paginationID)
			case bunpaginate.OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s <= ?", paginationColumn), paginationID)
			}
		}
	}

	return sb, nil
}

//nolint:unused
func (o columnPaginator[ResourceType, OptionsType]) BuildCursor(ret []ResourceType) (*bunpaginate.Cursor[ResourceType], error) {

	paginationColumn := o.query.Column

	pageSize := o.query.PageSize
	if pageSize == 0 {
		pageSize = bunpaginate.QueryDefaultPageSize
	}

	order := *o.query.Order

	var v ResourceType
	fields := findPaginationFieldPath(v, paginationColumn)

	var (
		paginationIDs = make([]*big.Int, 0)
	)
	for _, t := range ret {
		paginationID := findPaginationField(t, fields...)
		if o.query.Bottom == nil {
			o.query.Bottom = paginationID
		}
		paginationIDs = append(paginationIDs, paginationID)
	}

	hasMore := len(ret) > int(pageSize)
	if hasMore {
		ret = ret[:len(ret)-1]
	}
	if o.query.Reverse {
		for i := 0; i < len(ret)/2; i++ {
			ret[i], ret[len(ret)-i-1] = ret[len(ret)-i-1], ret[i]
		}
	}

	var previous, next *ColumnPaginatedQuery[OptionsType]

	if o.query.Reverse {
		cp := o.query
		cp.Reverse = false
		next = &cp

		if hasMore {
			cp := o.query
			cp.PaginationID = paginationIDs[len(paginationIDs)-2]
			previous = &cp
		}
	} else {
		if hasMore {
			cp := o.query
			cp.PaginationID = paginationIDs[len(paginationIDs)-1]
			next = &cp
		}
		if o.query.PaginationID != nil {
			if (order == bunpaginate.OrderAsc && o.query.PaginationID.Cmp(o.query.Bottom) > 0) ||
				(order == bunpaginate.OrderDesc && o.query.PaginationID.Cmp(o.query.Bottom) < 0) {
				cp := o.query
				cp.Reverse = true
				previous = &cp
			}
		}
	}

	return &bunpaginate.Cursor[ResourceType]{
		PageSize: int(pageSize),
		HasMore:  next != nil,
		Previous: encodeCursor[OptionsType, ColumnPaginatedQuery[OptionsType]](previous),
		Next:     encodeCursor[OptionsType, ColumnPaginatedQuery[OptionsType]](next),
		Data:     ret,
	}, nil
}

var _ Paginator[any] = &columnPaginator[any, any]{}

//nolint:unused
func findPaginationFieldPath(v any, paginationColumn string) []reflect.StructField {

	typeOfT := reflect.TypeOf(v)
	for i := 0; i < typeOfT.NumField(); i++ {
		field := typeOfT.Field(i)
		fieldType := field.Type

		// If the field is a pointer, we unreference it to target the concrete type
		// For example:
		// type Object struct {
		//     *AnotherObject
		// }
		for {
			if field.Type.Kind() == reflect.Ptr {
				fieldType = field.Type.Elem()
			}
			break
		}

		switch fieldType.Kind() {
		case reflect.Struct:
			if fieldType.AssignableTo(reflect.TypeOf(time.Time{})) ||
				fieldType.AssignableTo(reflect.TypeOf(libtime.Time{})) ||
				fieldType.AssignableTo(reflect.TypeOf(big.Int{})) ||
				fieldType.AssignableTo(reflect.TypeOf(bunpaginate.BigInt{})) {

				if fields := checkTag(field, paginationColumn); len(fields) > 0 {
					return fields
				}
			} else {
				fields := findPaginationFieldPath(reflect.New(fieldType).Elem().Interface(), paginationColumn)
				if len(fields) > 0 {
					return fields
				}
			}
		default:
			if fields := checkTag(field, paginationColumn); len(fields) > 0 {
				return fields
			}
		}
	}

	return nil
}

//nolint:unused
func checkTag(field reflect.StructField, paginationColumn string) []reflect.StructField {
	tag := field.Tag.Get("bun")
	column := strings.Split(tag, ",")[0]
	if column == paginationColumn {
		return []reflect.StructField{field}
	}

	return nil
}

//nolint:unused
func findPaginationField(v any, fields ...reflect.StructField) *big.Int {
	vOf := reflect.ValueOf(v)
	field := vOf.FieldByName(fields[0].Name)
	if len(fields) == 1 {
		switch rawPaginationID := field.Interface().(type) {
		case time.Time:
			return big.NewInt(rawPaginationID.UTC().UnixMicro())
		case *time.Time:
			return big.NewInt(rawPaginationID.UTC().UnixMicro())
		case *libtime.Time:
			return big.NewInt(rawPaginationID.UTC().UnixMicro())
		case libtime.Time:
			return big.NewInt(rawPaginationID.UTC().UnixMicro())
		case *bunpaginate.BigInt:
			return (*big.Int)(rawPaginationID)
		case bunpaginate.BigInt:
			return (*big.Int)(&rawPaginationID)
		case *big.Int:
			return rawPaginationID
		case big.Int:
			return &rawPaginationID
		case int64:
			return big.NewInt(rawPaginationID)
		case int:
			return big.NewInt(int64(rawPaginationID))
		case *int64:
			return big.NewInt(*rawPaginationID)
		case *int:
			return big.NewInt(int64(*rawPaginationID))
		case uint64:
			v := new(big.Int)
			v.SetUint64(rawPaginationID)
			return v
		case *uint64:
			v := new(big.Int)
			v.SetUint64(*rawPaginationID)
			return v
		default:
			panic(fmt.Sprintf("invalid paginationID, type %T not handled", rawPaginationID))
		}
	}

	return findPaginationField(v, fields[1:]...)
}

//nolint:unused
func encodeCursor[OptionsType any, PaginatedQueryType PaginatedQuery[OptionsType]](v *PaginatedQueryType) string {
	if v == nil {
		return ""
	}
	return bunpaginate.EncodeCursor(v)
}

func newColumnPaginator[ResourceType, OptionsType any](
	query ColumnPaginatedQuery[OptionsType],
	fieldName string,
	fieldType FieldType,
) columnPaginator[ResourceType, OptionsType] {
	return columnPaginator[ResourceType, OptionsType]{
		query:     query,
		fieldName: fieldName,
		fieldType: fieldType,
	}
}

func convertPaginationIDToSQLType(fieldType FieldType, id *big.Int) any {
	switch fieldType.(type) {
	case TypeDate:
		return libtime.UnixMicro(id.Int64())
	default:
		return id
	}
}
