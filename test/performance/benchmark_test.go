//go:build it

package performance_test

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"

	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

type Benchmark struct {
	EnvFactories map[string]EnvFactory
	Scripts      map[string]func(int) (string, map[string]string)

	reports map[string]map[string]*Report
	b       *testing.B
}

func (benchmark *Benchmark) Run(ctx context.Context) []*Report {
	reports := make([]*Report, 0)
	for envName, envFactory := range benchmark.EnvFactories {
		scriptsKeys := collectionutils.Keys(benchmark.Scripts)
		sort.Strings(scriptsKeys)

		for _, scriptName := range scriptsKeys {
			for _, features := range buildAllPossibleConfigurations() {

				testName := fmt.Sprintf("%s/%s/%s", envName, scriptName, features)

				ledgerConfiguration := ledger.Configuration{
					Features: features,
					Bucket:   uuid.NewString()[:8],
				}
				ledgerConfiguration.SetDefaults()
				report := newReport(ledgerConfiguration.Features, scriptName)

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
					b.ReportMetric(float64(report.AverageDuration().Milliseconds()), "ms/transaction")

					stopContext, cancel := context.WithTimeout(ctx, 10*time.Second)
					b.Cleanup(cancel)

					require.NoError(benchmark.b, env.Stop(stopContext))
				})

				if report.TransactionsCount > 0 {
					reports = append(reports, report)
				}
			}
		}
	}

	return reports
}

func New(b *testing.B, envFactories map[string]EnvFactory, scripts map[string]func(int) (string, map[string]string)) *Benchmark {
	return &Benchmark{
		b:            b,
		EnvFactories: envFactories,
		Scripts:      scripts,
		reports:      make(map[string]map[string]*Report),
	}
}
