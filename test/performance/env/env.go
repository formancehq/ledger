//go:build it

package env

import (
	"context"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
)

type Env interface {
	Client() *ledgerclient.Formance
	URL() string
	Stop(ctx context.Context) error
}

type EnvFactory interface {
	Create(ctx context.Context, b *testing.B, ledger ledger.Ledger) Env
}

var DefaultEnvFactory EnvFactory = nil
