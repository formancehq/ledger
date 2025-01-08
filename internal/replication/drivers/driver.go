package drivers

import (
	"context"

	"github.com/formancehq/ledger/internal/replication"
)

//go:generate mockgen -source driver.go -destination driver_generated.go -package drivers . Driver
type Driver interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Accept(ctx context.Context, logs ...replication.LogWithLedger) ([]error, error)
}
