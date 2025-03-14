package pagination

import (
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"math/big"
)

type PaginatedQuery[OptionsType any] interface {
	OffsetPaginatedQuery[OptionsType] | ColumnPaginatedQuery[OptionsType]
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
)
