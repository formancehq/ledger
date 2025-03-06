package runner

import (
	"context"
	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/storage/driver"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package runner . LogFetcher

type LogFetcher interface {
	ListLogs(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error)
}

type LogFetcherFn func(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error)

func (fn LogFetcherFn) ListLogs(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
	return fn(ctx, query)
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package runner . StorageDriver

type Storage interface {
	OpenLedger(context.Context, string) (LogFetcher, *ledger.Ledger, error)
	StorePipelineState(ctx context.Context, id string, lastLogID int) error
	ListEnabledPipelines(ctx context.Context) ([]ledger.Pipeline, error)
	GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error)
}

type storageAdapter struct {
	storageDriver *driver.Driver
	systemStore   *systemstore.DefaultStore
}

func (s *storageAdapter) GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error) {
	return s.systemStore.GetPipeline(ctx, id)
}

func (s *storageAdapter) OpenLedger(ctx context.Context, name string) (LogFetcher, *ledger.Ledger, error) {
	store, l, err := s.storageDriver.OpenLedger(ctx, name)

	return LogFetcherFn(func(ctx context.Context, query common.PaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error) {
		return store.Logs().Paginate(ctx, query)
	}), l, err
}

func (s *storageAdapter) StorePipelineState(ctx context.Context, id string, lastLogID int) error {
	return s.systemStore.StorePipelineState(ctx, id, lastLogID)
}

func (s *storageAdapter) ListEnabledPipelines(ctx context.Context) ([]ledger.Pipeline, error) {
	return s.systemStore.ListEnabledPipelines(ctx)
}

func NewStorageAdapter(storageDriver *driver.Driver, systemStore *systemstore.DefaultStore) Storage {
	return &storageAdapter{
		storageDriver: storageDriver,
		systemStore:   systemStore,
	}
}

var _ Storage = (*storageAdapter)(nil)
