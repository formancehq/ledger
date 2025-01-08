package controller

import (
	"context"
	"github.com/formancehq/ledger/internal"
)

//go:generate mockgen -source store.go -destination store_generated.go -package controller . Store
type Store interface {
	GetPipeline(ctx context.Context, id string) (*ledger.Pipeline, error)
}
