package ledger

import (
	"context"
	"testing"

	"github.com/alitto/pond"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/storage/sqlstorage/sqlstoragetesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func BenchmarkParallelWrites(b *testing.B) {
	driver := sqlstoragetesting.StorageDriver(b)
	resolver := NewResolver(driver)
	b.Cleanup(func() {
		require.NoError(b, resolver.CloseLedgers(context.Background()))
	})

	ledger, err := resolver.GetLedger(context.Background(), uuid.NewString())
	require.NoError(b, err)

	worker := pond.New(1000, 1000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		worker.Submit(func() {
			_, err := ledger.CreateTransaction(context.Background(), false, core.RunScript{
				Script: core.Script{
					Plain: `send [USD/2 100] (
					source = @world
					destination = @bank
				)`,
				},
			})
			require.NoError(b, err)
		})
	}
	worker.StopAndWait()
}
