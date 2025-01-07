package controller

import (
	"context"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ingester "github.com/formancehq/ledger/internal/replication"
)

//go:generate mockgen -source store.go -destination store_generated.go -package controller . Store
type Store interface {
	CreatePipeline(ctx context.Context, pipeline ingester.Pipeline) error
	DeletePipeline(ctx context.Context, id string) error
	GetPipeline(ctx context.Context, id string) (*ingester.Pipeline, error)
	ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ingester.Pipeline], error)

	ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ingester.Connector], error)
	CreateConnector(ctx context.Context, connector ingester.Connector) error
	DeleteConnector(ctx context.Context, id string) error
	GetConnector(ctx context.Context, id string) (*ingester.Connector, error)
}
