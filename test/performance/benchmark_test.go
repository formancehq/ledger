//go:build it

package performance_test

import (
	"context"
	"encoding/json"
	"fmt"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/ledger/pkg/generate"
	"net/http"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type ActionProvider interface {
	Get(iteration int) (*generate.Action, error)
}
type ActionProviderFn func(iteration int) (*generate.Action, error)

func (fn ActionProviderFn) Get(iteration int) (*generate.Action, error) {
	return fn(iteration)
}

type ActionProviderFactory interface {
	Create() (ActionProvider, error)
}

type ActionProviderFactoryFn func() (ActionProvider, error)

func (fn ActionProviderFactoryFn) Create() (ActionProvider, error) {
	return fn()
}

func NewJSActionProviderFactory(script string) ActionProviderFactoryFn {
	return func() (ActionProvider, error) {
		generator, err := generate.NewGenerator(script)
		if err != nil {
			return nil, err
		}

		return ActionProviderFn(func(iteration int) (*generate.Action, error) {
			return generator.Next(iteration)
		}), nil
	}
}

type Benchmark struct {
	EnvFactory EnvFactory
	Scenarios  map[string]ActionProviderFactory

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

				b.SetParallelism(int(parallelismFlag))
				b.ResetTimer()

				b.RunParallel(func(pb *testing.PB) {

					actionProvider, err := benchmark.Scenarios[scenario].Create()
					require.NoError(b, err)

					for pb.Next() {
						iteration := int(cpt.Add(1))

						action, err := actionProvider.Get(iteration)
						require.NoError(b, err)

						now := time.Now()

						_, err = action.Apply(ctx, env.Client().Ledger.V2, l.Name)
						require.NoError(b, err)

						report.registerTransactionLatency(time.Since(now))
					}
				})
				b.StopTimer()
				report.End = time.Now()

				// Fetch otel metrics
				rsp, err := http.Get(env.URL() + "/_/metrics")
				require.NoError(b, err)
				if rsp.StatusCode == http.StatusOK {
					ret := make(map[string]any)
					require.NoError(b, json.NewDecoder(rsp.Body).Decode(&ret))
					report.InternalMetrics = ret
				} else {
					b.Logf("Unable to fetch ledger metrics, got status code %d", rsp.StatusCode)
				}

				// Compute final results
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

func New(b *testing.B, envFactory EnvFactory, scenarios map[string]ActionProviderFactory) *Benchmark {
	return &Benchmark{
		b:          b,
		EnvFactory: envFactory,
		Scenarios:  scenarios,
		reports:    make(map[string]map[string]*report),
	}
}
