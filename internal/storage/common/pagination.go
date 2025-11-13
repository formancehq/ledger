package common

import (
	"math/big"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
)

type (
	InitialPaginatedQuery[OptionsType any] struct {
		Column   string                     `json:"column"`
		Order    *bunpaginate.Order         `json:"order"`
		PageSize uint64                     `json:"pageSize"`
		Options  ResourceQuery[OptionsType] `json:"filters"`
	}
	OffsetPaginatedQuery[OptionsType any] struct {
		InitialPaginatedQuery[OptionsType]
		Offset uint64 `json:"offset"`
	}
	ColumnPaginatedQuery[OptionsType any] struct {
		InitialPaginatedQuery[OptionsType]
		Bottom       *big.Int `json:"bottom"`
		PaginationID *big.Int `json:"paginationID"`
		Reverse      bool     `json:"reverse"`
	}
	PaginatedQuery[OptionsType any] interface {
		// Marker
		isPaginatedQuery()
	}
)

func (i InitialPaginatedQuery[OptionsType]) isPaginatedQuery() {}

var _ PaginatedQuery[any] = (*InitialPaginatedQuery[any])(nil)

var _ PaginatedQuery[any] = (*OffsetPaginatedQuery[any])(nil)

var _ PaginatedQuery[any] = (*ColumnPaginatedQuery[any])(nil)
