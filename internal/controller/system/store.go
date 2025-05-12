package system

import (
	"context"

	"github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger/internal/storage/common"

	ledgercontroller "github.com/formancehq/ledger/internal/controller/ledger"

	"github.com/formancehq/go-libs/v3/bun/bunpaginate"
	"github.com/formancehq/go-libs/v3/metadata"
	ledger "github.com/formancehq/ledger/internal"
)

type BucketWithStatus struct {
	Name      string     `json:"name"`
	DeletedAt *time.Time `json:"deletedAt,omitempty"`
}

type Store interface {
	GetLedger(ctx context.Context, name string) (*ledger.Ledger, error)
	ListLedgers(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[ledger.Ledger], error)
	UpdateLedgerMetadata(ctx context.Context, name string, m metadata.Metadata) error
	DeleteLedgerMetadata(ctx context.Context, param string, key string) error
	OpenLedger(context.Context, string) (ledgercontroller.Store, *ledger.Ledger, error)
	CreateLedger(context.Context, *ledger.Ledger) error
	MarkBucketAsDeleted(ctx context.Context, bucketName string) error
	RestoreBucket(ctx context.Context, bucketName string) error
	ListBucketsWithStatus(ctx context.Context, query common.ColumnPaginatedQuery[any]) (*bunpaginate.Cursor[BucketWithStatus], error)
}
