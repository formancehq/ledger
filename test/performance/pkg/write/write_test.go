//go:build it

package write_test

import (
	"context"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	. "github.com/formancehq/go-libs/v2/collectionutils"
	"github.com/formancehq/go-libs/v2/logging"
	"github.com/formancehq/go-libs/v2/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/formancehq/ledger/pkg/generate"
	"github.com/formancehq/ledger/test/performance/pkg/env"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
)

func init() {
	flag.StringVar(&scriptFlag, "script", "", "Script to run")
	flag.StringVar(&reportFileFlag, "report.file", "", "Location to write report file")
	flag.Int64Var(&parallelismFlag, "parallelism", 1, "Parallelism (default 1). Values is multiplied by GOMAXPROCS")
}

var (
	//go:embed scripts
	scriptsDir      embed.FS
	scripts         = map[string]ActionProviderFactory{}
	scriptFlag      string
	parallelismFlag int64
	reportFileFlag  string
)

type ActionProvider interface {
	Get(globalIteration, iteration int) (*generate.Action, error)
}
type ActionProviderFn func(globalIteration, iteration int) (*generate.Action, error)

func (fn ActionProviderFn) Get(globalIteration, iteration int) (*generate.Action, error) {
	return fn(globalIteration, iteration)
}

type ActionProviderFactory interface {
	Create() (ActionProvider, error)
}

type ActionProviderFactoryFn func() (ActionProvider, error)

func (fn ActionProviderFactoryFn) Create() (ActionProvider, error) {
	return fn()
}

func NewJSActionProviderFactory(rootPath, script string) ActionProviderFactoryFn {
	return func() (ActionProvider, error) {
		generator, err := generate.NewGenerator(script, generate.WithRootPath(rootPath))
		if err != nil {
			return nil, err
		}

		return ActionProviderFn(func(globalIteration, iteration int) (*generate.Action, error) {
			return generator.Next(iteration, generate.WithNextGlobals(map[string]any{
				"iteration": globalIteration,
			}))
		}), nil
	}
}

type writeBenchmark struct {
	EnvFactory env.EnvFactory
	Scenarios  map[string]ActionProviderFactory

	reports map[string]map[string]*report
	b       *testing.B
}

func (benchmark *writeBenchmark) Run(ctx context.Context) map[string][]Result {
	results := make(map[string][]Result, 0)
	scenarios := Keys(benchmark.Scenarios)
	sort.Strings(scenarios)

	for _, scenario := range scenarios {
		for _, configuration := range env.BuildAllPossibleConfigurations() {

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

				globalIteration := atomic.Int64{}

				env := env.Factory.Create(ctx, b, l)
				b.Logf("ledger: %s/%s", l.Bucket, l.Name)

				b.SetParallelism(int(parallelismFlag))
				b.ResetTimer()

				b.RunParallel(func(pb *testing.PB) {

					actionProvider, err := benchmark.Scenarios[scenario].Create()
					require.NoError(b, err)
					iteration := atomic.Int64{}

					for pb.Next() {
						globalIteration := int(globalIteration.Add(1))
						iteration := int(iteration.Add(1))

						action, err := actionProvider.Get(globalIteration, iteration)
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
				metrics, err := env.Client().Ledger.GetMetrics(ctx)
				if err != nil {
					b.Logf("Unable to fetch ledger metrics: %s", err)
				} else {
					report.InternalMetrics = metrics.Object
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

func newWriteBenchmark(b *testing.B, envFactory env.EnvFactory, scenarios map[string]ActionProviderFactory) *writeBenchmark {
	return &writeBenchmark{
		b:          b,
		EnvFactory: envFactory,
		Scenarios:  scenarios,
		reports:    make(map[string]map[string]*report),
	}
}

func BenchmarkWrite(b *testing.B) {

	// Load default scripts
	if scriptFlag == "" {
		entries, err := scriptsDir.ReadDir("scripts")
		require.NoError(b, err)

		for _, entry := range entries {
			if entry.IsDir() {
				continue
			}

			script, err := scriptsDir.ReadFile(filepath.Join("scripts", entry.Name()))
			require.NoError(b, err)

			rootPath, err := filepath.Abs("scripts")
			require.NoError(b, err)

			scripts[strings.TrimSuffix(entry.Name(), ".js")] = NewJSActionProviderFactory(rootPath, string(script))
		}
	} else {
		file, err := os.ReadFile(scriptFlag)
		require.NoError(b, err, "reading file "+scriptFlag)

		rootPath, err := filepath.Abs(filepath.Dir(scriptFlag))
		require.NoError(b, err)

		scripts["provided"] = NewJSActionProviderFactory(rootPath, string(file))
	}

	// Execute benchmarks
	reports := newWriteBenchmark(b, env.Factory, scripts).Run(logging.TestingContext())

	// Write report
	if reportFileFlag != "" {
		require.NoError(b, os.MkdirAll(filepath.Dir(reportFileFlag), 0755))

		f, err := os.Create(reportFileFlag)
		require.NoError(b, err)
		enc := json.NewEncoder(f)
		enc.SetIndent("", "  ")
		require.NoError(b, enc.Encode(reports))
	}
}

func TestMain(m *testing.M) {
	env.Start(m)
}
