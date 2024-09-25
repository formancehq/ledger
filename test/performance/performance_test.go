//go:build it

package benchmarks

import (
	"bytes"
	"fmt"
	"math/big"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/formancehq/go-libs/logging"
	"github.com/formancehq/go-libs/pointer"
	"github.com/formancehq/go-libs/testing/docker"
	"github.com/formancehq/go-libs/testing/platform/pgtesting"
	"github.com/formancehq/go-libs/testing/utils"
	"github.com/formancehq/go-libs/time"
	"github.com/formancehq/ledger/pkg/testserver"
	"github.com/formancehq/stack/ledger/client/models/components"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

var (
	dockerPool *docker.Pool
	srv        *pgtesting.PostgresServer
)

func TestMain(m *testing.M) {
	utils.WithTestMain(func(t *utils.TestingTForMain) int {
		dockerPool = docker.NewPool(t, logging.Testing())
		srv = pgtesting.CreatePostgresServer(t, dockerPool)

		return m.Run()
	})
}

func BenchmarkWorstCase(b *testing.B) {

	db := srv.NewDatabase(b)

	ctx := logging.TestingContext()

	ledgerName := uuid.NewString()
	testServer := testserver.New(b, testserver.Configuration{
		PostgresConfiguration: db.ConnectionOptions(),
		Debug:                 testing.Verbose(),
	})
	testServer.Start()
	defer testServer.Stop()

	_, err := testServer.Client().Ledger.V2.CreateLedger(ctx, ledgerName, &components.V2CreateLedgerRequest{})
	require.NoError(b, err)

	totalDuration := atomic.Int64{}
	b.SetParallelism(1000)
	runtime.GC()
	b.ResetTimer()
	startOfBench := time.Now()
	counter := atomic.Int64{}
	longestTxLock := sync.Mutex{}
	longestTransactionID := big.NewInt(0)
	longestTransactionDuration := time.Duration(0)
	b.RunParallel(func(pb *testing.PB) {
		buf := bytes.NewBufferString("")
		for pb.Next() {
			buf.Reset()
			id := counter.Add(1)
			now := time.Now()

			// todo: check why the generated sdk does not have the same signature as the global sdk
			transactionResponse, err := testServer.Client().Ledger.V2.CreateTransaction(ctx, ledgerName, components.V2PostTransaction{
				Timestamp: nil,
				Postings:  nil,
				Script: &components.V2PostTransactionScript{
					Plain: `vars {
	account $account
}

send [USD/2 100] (
	source = @world
	destination = $account
)`,
					Vars: map[string]any{
						"account": fmt.Sprintf("accounts:%d", id),
					},
				},

				Reference: nil,
				Metadata:  nil,
			}, pointer.For(false), nil)
			if err != nil {
				return
			}
			require.NoError(b, err)

			latency := time.Since(now).Milliseconds()
			totalDuration.Add(latency)

			longestTxLock.Lock()
			if time.Millisecond*time.Duration(latency) > longestTransactionDuration {
				longestTransactionID = transactionResponse.V2CreateTransactionResponse.Data.ID
				longestTransactionDuration = time.Duration(latency) * time.Millisecond
			}
			longestTxLock.Unlock()
		}
	})

	b.StopTimer()
	b.Logf("Longest transaction: %d (%s)", longestTransactionID, longestTransactionDuration.String())
	b.ReportMetric((float64(time.Duration(b.N))/float64(time.Since(startOfBench)))*float64(time.Second), "t/s")
	b.ReportMetric(float64(totalDuration.Load()/int64(b.N)), "ms/transaction")

	runtime.GC()
}
