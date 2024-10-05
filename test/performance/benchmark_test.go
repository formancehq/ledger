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

type Benchmark struct {
	EnvFactory EnvFactory
	Scripts    map[string]func(int) (string, map[string]string)

	reports map[string]map[string]*Report
	b       *testing.B
}

func (benchmark *Benchmark) Run(ctx context.Context) map[string][]Report {
	reports := make(map[string][]Report, 0)
	scriptsKeys := Keys(benchmark.Scripts)
	sort.Strings(scriptsKeys)

	for _, scriptName := range scriptsKeys {
		for _, configuration := range buildAllPossibleConfigurations() {

			testName := fmt.Sprintf("%s/%s", scriptName, configuration)

			ledgerConfiguration := ledger.Configuration{
				Features: configuration.FeatureSet,
				Bucket:   uuid.NewString()[:8],
			}
			ledgerConfiguration.SetDefaults()
			report := newReport(configuration, scriptName)

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

						script, vars := benchmark.Scripts[scriptName](iteration)
						now := time.Now()
						_, err := env.Executor().ExecuteScript(ctx, script, vars)
						require.NoError(b, err)

						report.registerTransactionLatency(time.Since(now))
					}
				})
				b.StopTimer()
				report.End = time.Now()

				b.ReportMetric(report.TPS(), "t/s")
				b.ReportMetric(float64(report.Tachymeter.Calc().Time.Avg.Milliseconds()), "ms/transaction")

				stopContext, cancel := context.WithTimeout(ctx, 10*time.Second)
				b.Cleanup(cancel)

				require.NoError(benchmark.b, env.Stop(stopContext))
			})

			if report.Tachymeter.Count > 0 {
				reports[scriptName] = append(reports[scriptName], report)
			}
		}
	}

	return reports
}

func New(b *testing.B, envFactory EnvFactory, scripts map[string]func(int) (string, map[string]string)) *Benchmark {
	return &Benchmark{
		b:          b,
		EnvFactory: envFactory,
		Scripts:    scripts,
		reports:    make(map[string]map[string]*Report),
	}
}
