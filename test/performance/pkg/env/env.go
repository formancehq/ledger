//go:build it

package env

import (
	"context"
	"testing"

	ledgerclient "github.com/formancehq/ledger/pkg/client"

	ledger "github.com/formancehq/ledger/internal"
)

type Env interface {
	Client() *ledgerclient.SDK
	URL() string
	Stop(ctx context.Context) error
}

type EnvFactory interface {
	Create(ctx context.Context, b *testing.B) Env
}

var FallbackEnvFactory EnvFactory = nil
