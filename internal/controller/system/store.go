package system

import (
	"context"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v2/bun/bunpaginate"
	"github.com/formancehq/go-libs/v2/metadata"
	ledger "github.com/formancehq/ledger/internal"
)

type Store interface {
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	ListLedgers(ctx context.Context, query ledgercontroller.ListLedgersQuery) (*bunpaginate.Cursor[ledger.Ledger], error)
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error
	OpenLedger(context.Context, string) (ledgercontroller.Store, *ledger.Ledger, error)
	CreateLedger(context.Context, *ledger.Ledger) error
}
