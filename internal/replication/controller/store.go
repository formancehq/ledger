package controller

import (
	"context"
	"github.com/formancehq/ledger/internal"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
)

//go:generate mockgen -source store.go -destination store_generated.go -package controller . Store
type Store interface {
	CreatePipeline(ctx context.Context, pipeline ledger.Pipeline) error
	DeletePipeline(ctx context.Context, id string) error
	GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error)
	ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error)
}
