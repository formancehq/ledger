//go:build it

package performance_test

import (
	"context"
	"testing"

	ledger "github.com/formancehq/ledger/internal"
)

type TransactionExecutor interface {
	ExecuteScript(context.Context, string, map[string]string) (*ledger.Transaction, error)
}
type TransactionExecutorFn func(context.Context, string, map[string]string) (*ledger.Transaction, error)

func (fn TransactionExecutorFn) ExecuteScript(ctx context.Context, script string, vars map[string]string) (*ledger.Transaction, error) {
	return fn(ctx, script, vars)
}

type Env interface {
	Executor() TransactionExecutor
	Stop(ctx context.Context) error
}

type EnvFactory interface {
	Create(ctx context.Context, b *testing.B, ledger ledger.Ledger) Env
}
