package pagination

import (
	"context"
	"encoding/json"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
	"math/big"
)

type PaginatedQueryOptions[T any] struct {
	QueryBuilder query.Builder `json:"qb"`
	PageSize     uint64        `json:"pageSize"`
	Options      T             `json:"options"`
}

func (opts *PaginatedQueryOptions[T]) UnmarshalJSON(data []byte) error {
	type aux struct {
		QueryBuilder json.RawMessage `json:"qb"`
		PageSize     uint64          `json:"pageSize"`
		Options      T               `json:"options"`
	}
	x := &aux{}
	if err := json.Unmarshal(data, x); err != nil {
		return err
	}

	*opts = PaginatedQueryOptions[T]{
		PageSize: x.PageSize,
		Options:  x.Options,
	}

	var err error
	if x.QueryBuilder != nil {
		opts.QueryBuilder, err = query.ParseJSON(string(x.QueryBuilder))
		if err != nil {
			return err
		}
	}

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
		PageSize: bunpaginate.QueryDefaultPageSize,
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

// //go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package ledger . Resource
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
