package system

import (
	"context"
	"time"

	"github.com/formancehq/go-libs/v3/metadata"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
	systemstore "github.com/formancehq/ledger/internal/storage/system"
)

type Store interface {
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	Ledgers() common.PaginatedResource[ledger.Ledger, systemstore.ListLedgersQueryPayload]
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error
	DeleteBucket(ctx context.Context, bucket string) error
	RestoreBucket(ctx context.Context, bucket string) error
	Sleep(ctx context.Context, duration time.Duration) error
}

type Driver interface {
	OpenLedger(context.Context, string) (ledgercontroller.Store, *ledger.Ledger, error)
	CreateLedger(context.Context, *ledger.Ledger) error
	GetSystemStore() Store
}
