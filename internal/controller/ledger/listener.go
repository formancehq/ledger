package ledger

import (
	"context"

	"github.com/formancehq/go-libs/metadata"
	ledger "github.com/formancehq/ledger/internal"
)

//go:generate mockgen -source listener.go -destination listener_generated.go -package ledger . Listener
type Listener interface {
	CommittedTransactions(ctx context.Context, ledger string, res ledger.Transaction, accountMetadata ledger.AccountMetadata)
	SavedMetadata(ctx context.Context, ledger string, targetType, id string, metadata metadata.Metadata)
	RevertedTransaction(ctx context.Context, ledger string, reverted, revert ledger.Transaction)
	DeletedMetadata(ctx context.Context, ledger string, targetType string, targetID any, key string)
}
