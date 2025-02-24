package pagination

import (
	"context"
	"encoding/json"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/query"
	"github.com/formancehq/go-libs/v2/time"
)

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

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source resource.go -destination resource_generated_test.go -package pagination . Resource
type Resource[ResourceType, OptionsType any] interface {
	GetOne(ctx context.Context, query ResourceQuery[OptionsType]) (*ResourceType, error)
	Count(ctx context.Context, query ResourceQuery[OptionsType]) (int, error)
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source resource.go -destination resource_generated_test.go -package pagination . PaginatedResource
type PaginatedResource[ResourceType, OptionsType any, PaginationQueryType PaginatedQuery[OptionsType]] interface {
	Resource[ResourceType, OptionsType]
	Paginate(ctx context.Context, paginationOptions PaginationQueryType) (*bunpaginate.Cursor[ResourceType], error)
}
