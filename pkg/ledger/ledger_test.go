package ledger_test

import (
	"context"
	"flag"
	"os"
	"testing"
	"time"

	"github.com/numary/ledger/pkg/ledger"
	"github.com/numary/ledger/pkg/ledgertesting"
	"github.com/numary/ledger/pkg/storage"
	"github.com/pborman/uuid"
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/require"
	"go.uber.org/fx"
)

func withContainer(options ...fx.Option) {
	done := make(chan struct{})
	opts := append([]fx.Option{
		fx.NopLogger,
		ledgertesting.ProvideLedgerStorageDriver(),
	}, options...)
	opts = append(opts, fx.Invoke(func(lc fx.Lifecycle) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				close(done)
				return nil
			},
		})
	}))
	app := fx.New(opts...)
	go func() {
		if err := app.Start(context.Background()); err != nil {
			panic(err)
		}
	}()

	<-done
	if app.Err() != nil {
		panic(app.Err())
	}

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(5*time.Second))
	defer cancel()

	if err := app.Stop(ctx); err != nil {
		panic(err)
	}
}

func runOnLedger(f func(l *ledger.Ledger), ledgerOptions ...ledger.LedgerOption) {
	withContainer(fx.Invoke(func(lc fx.Lifecycle, storageDriver storage.Driver[ledger.Store]) {
		lc.Append(fx.Hook{
			OnStart: func(ctx context.Context) error {
				name := uuid.New()
				store, _, err := storageDriver.GetLedgerStore(context.Background(), name, true)
				if err != nil {
					return err
				}
				_, err = store.Initialize(context.Background())
				if err != nil {
					return err
				}
				l, err := ledger.NewLedger(store, ledger.NewNoOpMonitor(), ledgerOptions...)
				if err != nil {
					panic(err)
				}
				lc.Append(fx.Hook{
					OnStop: l.Close,
				})
				f(l)
				return nil
			},
		})
	}))
}

func TestMain(m *testing.M) {
	var code int
	defer func() {
		os.Exit(code) // os.Exit don't care about defer so defer the os.Exit allow us to execute other defer
	}()

	flag.Parse()
	if testing.Verbose() {
		logrus.StandardLogger().Level = logrus.DebugLevel
	}

	code = m.Run()
}

func BenchmarkGetAccount(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		for i := 0; i < b.N; i++ {
			_, err := l.GetAccount(context.Background(), "users:013")
			require.NoError(b, err)
		}
	})
}

func BenchmarkGetTransactions(b *testing.B) {
	runOnLedger(func(l *ledger.Ledger) {
		for i := 0; i < b.N; i++ {
			_, err := l.GetTransactions(context.Background(), ledger.TransactionsQuery{})
			require.NoError(b, err)
		}
	})
}
