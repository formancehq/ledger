package replication

import (
	"context"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/storage/driver"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package replication . LogFetcher

type LogFetcher interface {
	ListLogs(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error)
}

type LogFetcherFn func(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error)

func (fn LogFetcherFn) ListLogs(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
	return fn(ctx, query)
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package replication . StorageDriver

type Storage interface {
	OpenLedger(context.Context, string) (LogFetcher, *ledger.Ledger, error)
	StorePipelineState(ctx context.Context, id string, lastLogID int) error

	ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error)
	CreateConnector(ctx context.Context, connector ledger.Connector) error
	DeleteConnector(ctx context.Context, id string) error
	GetConnector(ctx context.Context, id string) (*ledger.Connector, error)

	CreatePipeline(ctx context.Context, pipeline ledger.Pipeline) error
	DeletePipeline(ctx context.Context, id string) error
	UpdatePipeline(ctx context.Context, id string, o map[string]any) (*ledger.Pipeline, error)
	ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error)
	ListEnabledPipelines(ctx context.Context) ([]ledger.Pipeline, error)
	GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error)
}

type storageAdapter struct {
	*systemstore.DefaultStore
	storageDriver *driver.Driver
}

func (s *storageAdapter) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	return s.DefaultStore.GetPipeline(ctx, id)
}

func (s *storageAdapter) OpenLedger(ctx context.Context, name string) (LogFetcher, *ledger.Ledger, error) {
	store, l, err := s.storageDriver.OpenLedger(ctx, name)

	return LogFetcherFn(func(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
		return store.Logs().Paginate(ctx, query)
	}), l, err
}

func (s *storageAdapter) StorePipelineState(ctx context.Context, id string, lastLogID int) error {
	return s.DefaultStore.StorePipelineState(ctx, id, lastLogID)
}

func (s *storageAdapter) ListEnabledPipelines(ctx context.Context) ([]ledger.Pipeline, error) {
	return s.DefaultStore.ListEnabledPipelines(ctx)
}

func NewStorageAdapter(storageDriver *driver.Driver, systemStore *systemstore.DefaultStore) Storage {
	return &storageAdapter{
		storageDriver: storageDriver,
		DefaultStore:  systemStore,
	}
}

var _ Storage = (*storageAdapter)(nil)
