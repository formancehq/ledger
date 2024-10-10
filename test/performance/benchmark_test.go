//go:build it

package performance_test

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"

	. "github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type TransactionProvider interface {
	Get(iteration int) (string, map[string]string)
}
type TransactionProviderFn func(iteration int) (string, map[string]string)
func (fn TransactionProviderFn) Get(iteration int) (string, map[string]string) {
	return fn(iteration)
}

type Benchmark struct {
	EnvFactory EnvFactory
	Scenarios  map[string]TransactionProvider

	reports map[string]map[string]*report
	b       *testing.B
}

func (benchmark *Benchmark) Run(ctx context.Context) map[string][]Result {
	results := make(map[string][]Result, 0)
	scenarios := Keys(benchmark.Scenarios)
	sort.Strings(scenarios)

	for _, scenario := range scenarios {
		for _, configuration := range buildAllPossibleConfigurations() {

			testName := fmt.Sprintf("%s/%s", scenario, configuration)

			ledgerConfiguration := ledger.Configuration{
				Features: configuration.FeatureSet,
				Bucket:   uuid.NewString()[:8],
			}
			ledgerConfiguration.SetDefaults()
			report := newReport(configuration, scenario)
			var result Result

			benchmark.b.Run(testName, func(b *testing.B) {
				report.reset()
				l := ledger.Ledger{
					Configuration: ledgerConfiguration,
					Name:          uuid.NewString()[:8],
				}

				cpt := atomic.Int64{}

				env := envFactory.Create(ctx, b, l)
				b.Logf("ledger: %s/%s", l.Bucket, l.Name)

				b.SetParallelism(int(parallelism))
				b.ResetTimer()
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						iteration := int(cpt.Add(1))

						script, vars := benchmark.Scenarios[scenario].Get(iteration)
						now := time.Now()
						_, err := env.Executor().ExecuteScript(ctx, script, vars)
						require.NoError(b, err)

						report.registerTransactionLatency(time.Since(now))
					}
				})
				b.StopTimer()
				report.End = time.Now()
				result = report.GetResult()

				b.ReportMetric(report.TPS(), "t/s")
				b.ReportMetric(float64(result.Metrics.Time.Avg.Milliseconds()), "ms/transaction")

				stopContext, cancel := context.WithTimeout(ctx, 10*time.Second)
				b.Cleanup(cancel)

				require.NoError(benchmark.b, env.Stop(stopContext))
			})

			if report.Tachymeter.Count > 0 {
				results[scenario] = append(results[scenario], result)
			}
		}
	}

	return results
}

func New(b *testing.B, envFactory EnvFactory, scenarios map[string]TransactionProvider) *Benchmark {
	return &Benchmark{
		b:          b,
		EnvFactory: envFactory,
		Scenarios:  scenarios,
		reports:    make(map[string]map[string]*report),
	}
}
