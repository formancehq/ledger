package bunpaginate

import (
	"encoding/json"

	"github.com/formancehq/go-libs/query"
)

type PaginatedQueryOptions[T any] struct {
	QueryBuilder query.Builder `json:"qb"`
	PageSize     uint64        `json:"pageSize"`
	Options      T             `json:"options"`
}

func (pqo *PaginatedQueryOptions[T]) UnmarshalJSON(data []byte) error {
	type base struct {
		PageSize uint64 `json:"pageSize"`
		Options  T      `json:"options"`
	}
	var b base
	if err := json.Unmarshal(data, &b); err != nil {
		return err
	}
	pqo.PageSize = b.PageSize
	pqo.Options = b.Options
	var dataMap map[string]json.RawMessage
	if err := json.Unmarshal(data, &dataMap); err != nil {
		return err
	}
	qb, err := query.ParseJSON(string(dataMap["qb"]))
	if err != nil {
		return err
	}
	pqo.QueryBuilder = qb
	return nil
}

func (opts PaginatedQueryOptions[T]) WithQueryBuilder(qb query.Builder) PaginatedQueryOptions[T] {
	opts.QueryBuilder = qb

	return opts
}

func (opts PaginatedQueryOptions[T]) WithPageSize(pageSize uint64) PaginatedQueryOptions[T] {
	opts.PageSize = pageSize

	return opts
}

func NewPaginatedQueryOptions[T any](options T) PaginatedQueryOptions[T] {
	return PaginatedQueryOptions[T]{
		Options:  options,
		PageSize: QueryDefaultPageSize,
	}
}
