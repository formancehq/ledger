package system

import (
	"context"

	"github.com/formancehq/go-libs/v5/pkg/types/metadata"

	ledger "github.com/formancehq/ledger/internal"
	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"
	"github.com/formancehq/ledger/internal/storage/common"
	"github.com/formancehq/ledger/internal/storage/system"
)

// Store is the read-only view of _system that the controller receives via
// GetSystemStore. All mutation paths must go through the cache-aware Driver
// methods (UpdateLedgerMetadata, DeleteLedgerMetadata, DeleteBucket,
// RestoreBucket) so that cache eviction is never bypassed.
type Store interface {
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	Ledgers() common.PaginatedResource[ledger.Ledger, system.ListLedgersQueryPayload]
}

type Driver interface {
	OpenLedger(context.Context, string) (ledgercontroller.Store, *ledger.Ledger, error)
	CreateLedger(context.Context, *ledger.Ledger) error
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	DeleteLedgerMetadata(ctx context.Context, name string, key string) error
	DeleteBucket(ctx context.Context, bucket string) error
	RestoreBucket(ctx context.Context, bucket string) error
	GetSystemStore() Store
}
