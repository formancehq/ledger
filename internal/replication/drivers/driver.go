package drivers

import (
	"context"
)

//go:generate mockgen -source driver.go -destination driver_generated.go -package drivers . Driver
type Driver interface {
	Start(ctx context.Context) error
	Stop(ctx context.Context) error
	Accept(ctx context.Context, logs ...LogWithLedger) ([]error, error)
}
