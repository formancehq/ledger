package system

import (
	"context"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/storage/system"

	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger/internal"
)

type Store interface {
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	Ledgers() common.PaginatedResource[ledger.Ledger, system.ListLedgersQueryPayload]
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error
}

type Driver interface {
	OpenLedger(context.Context, string) (ledgercontroller.Store, *ledger.Ledger, error)
	CreateLedger(context.Context, *ledger.Ledger) error
	GetSystemStore() Store
}
