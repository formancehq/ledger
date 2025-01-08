package runner

import (
	"context"
	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/internal/pagination"
)

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package runner . LogFetcher

type LogFetcher interface {
	ListLogs(ctx context.Context, query pagination.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Log], error)
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package runner . StorageDriver

type StorageDriver interface {
	OpenLedger(context.Context, string) (LogFetcher, *ledger.Ledger, error)
}

//go:generate mockgen -write_source_comment=false -write_package_comment=false -source store.go -destination store_generated_test.go -package runner . SystemStore

type SystemStore interface {
	StorePipelineState(ctx context.Context, id string, state ledger.PipelineState) error
	ListEnabledPipelines(ctx context.Context) ([]ledger.Pipeline, error)
}
