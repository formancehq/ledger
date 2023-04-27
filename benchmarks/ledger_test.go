package benchmarks

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"runtime"
	"testing"
	"time"

	"github.com/formancehq/ledger/pkg/api/controllers"
	"github.com/formancehq/ledger/pkg/api/routes"
	"github.com/formancehq/ledger/pkg/core"
	"github.com/formancehq/ledger/pkg/ledger"
	"github.com/formancehq/ledger/pkg/opentelemetry/metrics"
	"github.com/formancehq/ledger/pkg/storage/sqlstoragetesting"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func BenchmarkParallelWrites(b *testing.B) {

	driver := sqlstoragetesting.StorageDriver(b)
	resolver := ledger.NewResolver(driver)
	b.Cleanup(func() {
		require.NoError(b, resolver.CloseLedgers(context.Background()))
	})

	ledgerName := uuid.NewString()

	backend := controllers.NewDefaultBackend(driver, "latest", resolver)
	router := routes.NewRouter(backend, nil, nil, metrics.NewNoOpMetricsRegistry())
	srv := httptest.NewServer(router)
	defer srv.Close()

	r := rand.New(rand.NewSource(0))

	totalDuration := atomic.Int64{}
	b.SetParallelism(1000)
	runtime.GC()
	b.ResetTimer()
	startOfBench := time.Now()
	b.RunParallel(func(pb *testing.PB) {
		buf := bytes.NewBufferString("")
		for pb.Next() {
			buf.Reset()

			err := json.NewEncoder(buf).Encode(controllers.PostTransactionRequest{
				Script: core.Script{
					Plain: fmt.Sprintf(`send [USD/2 100] (
						source = @world
						destination = @accounts:%d
					)`, r.Int()%100),
				},
			})
			require.NoError(b, err)

			req := httptest.NewRequest("POST", "/"+ledgerName+"/transactions", buf)
			req.URL.RawQuery = url.Values{
				"async": []string{os.Getenv("ASYNC")},
			}.Encode()
			rsp := httptest.NewRecorder()

			now := time.Now()
			router.ServeHTTP(rsp, req)
			totalDuration.Add(time.Since(now).Milliseconds())

			require.Equal(b, http.StatusOK, rsp.Code)
		}
	})
	b.StopTimer()
	b.ReportMetric((float64(time.Duration(b.N))/float64(time.Since(startOfBench)))*float64(time.Second), "t/s")
	b.ReportMetric(float64(totalDuration.Load()/int64(b.N)), "ms/transaction")
	runtime.GC()
}
