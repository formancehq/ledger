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

type ColumnPaginator[ResourceType, OptionsType any] struct {
	DefaultPaginationColumn string
	DefaultOrder            bunpaginate.Order
}

//nolint:unused
func (o ColumnPaginator[ResourceType, OptionsType]) Paginate(sb *bun.SelectQuery, query ColumnPaginatedQuery[OptionsType]) (*bun.SelectQuery, error) {

	paginationColumn := o.DefaultPaginationColumn
	originalOrder := o.DefaultOrder
	if query.Order != nil {
		originalOrder = *query.Order
	}

	pageSize := query.PageSize
	if pageSize == 0 {
		pageSize = bunpaginate.QueryDefaultPageSize
	}

	sb = sb.Limit(int(pageSize) + 1) // Fetch one additional item to find the next token
	order := originalOrder
	if query.Reverse {
		order = originalOrder.Reverse()
	}
	orderExpression := fmt.Sprintf("%s %s", paginationColumn, order)
	sb = sb.ColumnExpr("row_number() OVER (ORDER BY " + orderExpression + ")")

	if query.PaginationID != nil {
		if query.Reverse {
			switch originalOrder {
			case bunpaginate.OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s < ?", paginationColumn), query.PaginationID)
			case bunpaginate.OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s > ?", paginationColumn), query.PaginationID)
			}
		} else {
			switch originalOrder {
			case bunpaginate.OrderAsc:
				sb = sb.Where(fmt.Sprintf("%s >= ?", paginationColumn), query.PaginationID)
			case bunpaginate.OrderDesc:
				sb = sb.Where(fmt.Sprintf("%s <= ?", paginationColumn), query.PaginationID)
			}
		}
	}

	return sb, nil
}

//nolint:unused
func (o ColumnPaginator[ResourceType, OptionsType]) BuildCursor(ret []ResourceType, query ColumnPaginatedQuery[OptionsType]) (*bunpaginate.Cursor[ResourceType], error) {

	paginationColumn := query.Column
	if paginationColumn == "" {
		paginationColumn = o.DefaultPaginationColumn
	}

	pageSize := query.PageSize
	if pageSize == 0 {
		pageSize = bunpaginate.QueryDefaultPageSize
	}

	order := o.DefaultOrder
	if query.Order != nil {
		order = *query.Order
	}

	var v ResourceType
	fields := findPaginationFieldPath(v, paginationColumn)

	var (
		paginationIDs = make([]*big.Int, 0)
	)
	for _, t := range ret {
		paginationID := findPaginationField(t, fields...)
		if query.Bottom == nil {
			query.Bottom = paginationID
		}
		paginationIDs = append(paginationIDs, paginationID)
	}

	hasMore := len(ret) > int(pageSize)
	if hasMore {
		ret = ret[:len(ret)-1]
	}
	if query.Reverse {
		for i := 0; i < len(ret)/2; i++ {
			ret[i], ret[len(ret)-i-1] = ret[len(ret)-i-1], ret[i]
		}
	}

	var previous, next *ColumnPaginatedQuery[OptionsType]

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
			if (order == bunpaginate.OrderAsc && query.PaginationID.Cmp(query.Bottom) > 0) || (order == bunpaginate.OrderDesc && query.PaginationID.Cmp(query.Bottom) < 0) {
				cp := query
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

var _ Paginator[any, ColumnPaginatedQuery[any]] = &ColumnPaginator[any, any]{}

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
	if len(fields) == 0 {
		return big.NewInt(0)
	}
	
	vOf := reflect.ValueOf(v)
	field := vOf.FieldByName(fields[0].Name)
	if len(fields) == 1 {
		switch rawPaginationID := field.Interface().(type) {
		case time.Time:
			return big.NewInt(rawPaginationID.UTC().UnixMicro())
		case *time.Time:
			if rawPaginationID == nil {
				return big.NewInt(0)
			}
			return big.NewInt(rawPaginationID.UTC().UnixMicro())
		case *libtime.Time:
			if rawPaginationID == nil {
				return big.NewInt(0)
			}
			return big.NewInt(rawPaginationID.UTC().UnixMicro())
		case libtime.Time:
			return big.NewInt(rawPaginationID.UTC().UnixMicro())
		case *bunpaginate.BigInt:
			if rawPaginationID == nil {
				return big.NewInt(0)
			}
			return (*big.Int)(rawPaginationID)
		case bunpaginate.BigInt:
			return (*big.Int)(&rawPaginationID)
		case *big.Int:
			if rawPaginationID == nil {
				return big.NewInt(0)
			}
			return rawPaginationID
		case big.Int:
			return &rawPaginationID
		case int64:
			return big.NewInt(rawPaginationID)
		case int:
			return big.NewInt(int64(rawPaginationID))
		case *int64:
			if rawPaginationID == nil {
				return big.NewInt(0)
			}
			return big.NewInt(*rawPaginationID)
		case *int:
			if rawPaginationID == nil {
				return big.NewInt(0)
			}
			return big.NewInt(int64(*rawPaginationID))
		default:
			return big.NewInt(0)
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
