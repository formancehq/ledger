package aggregator

import (
	"context"

	"github.com/formancehq/ledger/pkg/core"
)

type Store interface {
	GetAccountWithVolumes(ctx context.Context, address string) (*core.AccountWithVolumes, error)
}
