package benchmarks

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/storagetesting"
	"github.com/formancehq/stack/libs/go-libs/api"
	"github.com/formancehq/stack/libs/go-libs/logging"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func BenchmarkParallelWrites(b *testing.B) {

	ctx := logging.TestingContext()

	driver := storagetesting.StorageDriver(b)
	resolver := ledger.NewResolver(driver, ledger.WithLogger(logging.FromContext(ctx)))
	b.Cleanup(func() {
		require.NoError(b, resolver.CloseLedgers(ctx))
	})

	ledgerName := uuid.NewString()

	backend := controllers.NewDefaultBackend(driver, "latest", resolver)
	router := routes.NewRouter(backend, nil, metrics.NewNoOpRegistry())
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx := logging.ContextWithLogger(r.Context(), logging.FromContext(ctx))
		router.ServeHTTP(w, r.WithContext(ctx))
	})

	totalDuration := atomic.Int64{}
	b.SetParallelism(1000)
	runtime.GC()
	b.ResetTimer()
	startOfBench := time.Now()
	counter := atomic.NewInt64(0)
	longestTxLock := sync.Mutex{}
	longestTransactionID := uint64(0)
	longestTransactionDuration := time.Duration(0)
	b.RunParallel(func(pb *testing.PB) {
		buf := bytes.NewBufferString("")
		for pb.Next() {
			buf.Reset()
			id := counter.Add(1)

			//script := controllers.Script{
			//	Script: core.Script{
			//		Plain: fmt.Sprintf(`
			//			vars {
			//				account $account
			//			}
			//
			//			send [USD/2 100] (
			//				source = @world:%d allowing unbounded overdraft
			//				destination = $account
			//			)`, counter.Load()%100),
			//	},
			//	Vars: map[string]any{
			//		"account": fmt.Sprintf("accounts:%d", counter.Add(1)),
			//	},
			//}

			script := controllers.Script{
				Script: core.Script{
					Plain: `vars {
	account $account
}

send [USD/2 100] (
	source = @world
	destination = $account
)`,
				},
				Vars: map[string]any{
					"account": fmt.Sprintf("accounts:%d", id),
				},
			}

			//			script := controllers.Script{
			//				Script: core.Script{
			//					Plain: `vars {
			//	account $account
			//	account $src
			//}
			//
			//send [USD/2 100] (
			//	source = $src allowing unbounded overdraft
			//	destination = $account
			//)`,
			//				},
			//				Vars: map[string]any{
			//					"src":     fmt.Sprintf("world:%d", id),
			//					"account": fmt.Sprintf("accounts:%d", id),
			//				},
			//			}

			err := json.NewEncoder(buf).Encode(controllers.PostTransactionRequest{
				Script: script,
			})
			require.NoError(b, err)

			//ctx, _ := context.WithDeadline(ctx, time.Now().Add(10*time.Second))

			req := httptest.NewRequest("POST", "/"+ledgerName+"/transactions", buf)
			req = req.WithContext(ctx)
			req.URL.RawQuery = url.Values{
				"async": []string{os.Getenv("ASYNC")},
			}.Encode()
			rsp := httptest.NewRecorder()

			now := time.Now()
			handler.ServeHTTP(rsp, req)
			latency := time.Since(now).Milliseconds()
			totalDuration.Add(latency)

			require.Equal(b, http.StatusOK, rsp.Code)
			tx, _ := api.DecodeSingleResponse[core.Transaction](b, rsp.Body)

			longestTxLock.Lock()
			if time.Millisecond*time.Duration(latency) > longestTransactionDuration {
				longestTransactionID = tx.ID
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
