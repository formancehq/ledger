package drivers

import (
	"context"
	ledger "github.com/formancehq/ledger/internal"
)

//go:generate mockgen -source store.go -destination store_generated.go -package drivers . Store
type Store interface {
	GetConnector(ctx context.Context, id string) (*ledger.Connector, error)
}
