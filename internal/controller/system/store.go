package system

import (
	"context"
	systemstore "github.com/formancehq/ledger/internal/storage/system"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/metadata"
	ledger "github.com/formancehq/ledger/internal"
)

type Store interface {
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	ListLedgers(ctx context.Context, query systemstore.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error)
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error

	ListConnectors(ctx context.Context) (*bunpaginate.Cursor[ledger.Connector], error)
	CreateConnector(ctx context.Context, connector ledger.Connector) error
	DeleteConnector(ctx context.Context, id string) error
	GetConnector(ctx context.Context, id string) (*ledger.Connector, error)

	CreatePipeline(ctx context.Context, pipeline ledger.Pipeline) error
	DeletePipeline(ctx context.Context, id string) error
	UpdatePipeline(ctx context.Context, id string, o map[string]any) error
	GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error)
	ListPipelines(ctx context.Context) (*bunpaginate.Cursor[ledger.Pipeline], error)
}

type Driver interface {
	OpenLedger(context.Context, string) (ledgercontroller.Store, *ledger.Ledger, error)
	CreateLedger(context.Context, *ledger.Ledger) error
	GetSystemStore() Store
}
