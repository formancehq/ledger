//go:build it

package performance_test

import (
	"context"
	"fmt"
	"github.com/formancehq/go-libs/collectionutils"
	"github.com/formancehq/go-libs/time"
	ledger "github.com/formancehq/ledger/internal"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
)

type Iterator interface {
	Next() bool
}

type RunConfiguration struct {
	Env      string
	Script   string
	Features FeatureConfiguration
	Ledger   ledger.Ledger
}

func (c RunConfiguration) String() string {
	return fmt.Sprintf("%s/%s/%s/%s/%s", c.Env, c.Script, c.Features, c.Ledger.Bucket, c.Ledger.Name)
}

type Benchmark struct {
	mu sync.Mutex

	EnvFactories map[string]EnvFactory
	Scripts      map[string]func(int) (string, map[string]string)

	reports map[string]map[string]*Report
	b       *testing.B
}

func (benchmark *Benchmark) addReport(env, script string, report *Report) {
	benchmark.mu.Lock()
	defer benchmark.mu.Unlock()

	if benchmark.reports[env] == nil {
		benchmark.reports[env] = make(map[string]*Report)
	}
	benchmark.reports[env][script] = report
}

func (benchmark *Benchmark) Run(ctx context.Context) error {
	for envName, envFactory := range benchmark.EnvFactories {
		scriptsKeys := collectionutils.Keys(benchmark.Scripts)
		sort.Strings(scriptsKeys)

		for _, scriptName := range scriptsKeys {
			for _, features := range buildAllPossibleConfigurations() {

				testName := fmt.Sprintf("%s/%s/%s", envName, scriptName, features)

				benchmark.b.Run(testName, func(b *testing.B) {
					ledgerConfiguration := ledger.Configuration{
						Features: features,
						Bucket:   uuid.NewString()[:8],
					}
					ledgerConfiguration.SetDefaults()
					l := ledger.Ledger{
						Configuration: ledgerConfiguration,
						Name:          uuid.NewString()[:8],
					}

					cpt := atomic.Int64{}
					report := newReport()

					env := envFactory.Create(ctx, b, l)
					b.Logf("ledger: %s/%s", l.Bucket, l.Name)

					b.SetParallelism(int(parallelism))
					b.ResetTimer()
					b.RunParallel(func(pb *testing.PB) {
						for pb.Next() {
							id := int(cpt.Add(1))

							script, vars := benchmark.Scripts[scriptName](id)
							now := time.Now()
							_, err := env.Executor().ExecuteScript(ctx, script, vars)
							require.NoError(b, err)

							report.registerTransactionLatency(id, time.Since(now), err)
						}
					})
					b.StopTimer()
					report.endOfBench = time.Now()

					b.ReportMetric(report.TPS(), "t/s")
					b.ReportMetric(float64(report.AverageDuration().Milliseconds()), "ms/transaction")

					require.NoError(benchmark.b, env.Stop())
				})
			}
		}
	}

	return nil
}

func New(b *testing.B, envFactories map[string]EnvFactory, scripts map[string]func(int) (string, map[string]string)) *Benchmark {
	return &Benchmark{
		b:            b,
		EnvFactories: envFactories,
		Scripts:      scripts,
		reports:      make(map[string]map[string]*Report),
	}
}
