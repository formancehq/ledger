package common

import (
	"encoding/json"
	"math/big"

	"github.com/formancehq/go-libs/v4/bun/bunpaginate"
)

type PaginationConfig struct {
	MaxPageSize     uint64
	DefaultPageSize uint64
}

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

func UnmarshalInitialPaginatedQueryOpts[TO any](from InitialPaginatedQuery[map[string]any]) (*InitialPaginatedQuery[TO], error) {
	var opts TO
	marshalled, err := json.Marshal(from.Options.Opts)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(marshalled, &opts); err != nil {
		return nil, err
	} else {
		return &InitialPaginatedQuery[TO]{
			PageSize: from.PageSize,
			Column:   from.Column,
			Order:    from.Order,
			Options: ResourceQuery[TO]{
				PIT:     from.Options.PIT,
				OOT:     from.Options.OOT,
				Builder: from.Options.Builder,
				Expand:  from.Options.Expand,
				Opts:    opts,
			},
		}, nil
	}
}
