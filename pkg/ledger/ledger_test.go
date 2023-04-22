package ledger

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger/command"
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

	r := rand.New(rand.NewSource(0))

	b.SetParallelism(1000)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := ledger.CreateTransaction(context.Background(), command.Parameters{
				Async: os.Getenv("ASYNC") == "true",
			}, core.RunScript{
				Script: core.Script{
					Plain: fmt.Sprintf(`send [USD/2 100] (
						source = @world
						destination = @accounts:%d
					)`, r.Int()%100),
				},
			})
			require.NoError(b, err)
		}
	})
	b.StopTimer()
}
