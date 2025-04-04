//go:build it

package env

import (
	"context"
	ledgerclient "github.com/formancehq/ledger/pkg/client"
	"testing"
)

type Env interface {
	Client() *ledgerclient.Formance
	Stop(ctx context.Context) error
}

type EnvFactory interface {
	Create(ctx context.Context, b *testing.B) Env
}

var FallbackEnvFactory EnvFactory = nil
