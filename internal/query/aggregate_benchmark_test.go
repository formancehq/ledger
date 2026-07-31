package query_test

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/query"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

var benchmarkAggregateResult *commonpb.AggregateResult

func BenchmarkAggregateAllVolumes(b *testing.B) {
	for _, volumeCount := range []int{1_000, 10_000, 100_000} {
		b.Run(fmt.Sprintf("volumes=%d", volumeCount), func(b *testing.B) {
			store := newBenchmarkStore(b)
			attrs := attributes.New()

			seedBenchmarkVolumes(b, store, attrs, volumeCount)
			if err := store.Flush(); err != nil {
				b.Fatalf("flushing benchmark store: %v", err)
			}

			handle, err := store.NewReadHandle()
			if err != nil {
				b.Fatalf("opening benchmark read handle: %v", err)
			}
			b.Cleanup(func() { _ = handle.Close() })

			// Populate Pebble's block cache before measuring the hot-read baseline.
			if _, err := query.AggregateAllVolumes(handle, attrs.Volume, "benchmark", query.AggregateOptions{}); err != nil {
				b.Fatalf("warming aggregate query: %v", err)
			}

			b.ReportAllocs()
			b.ReportMetric(float64(volumeCount), "volume_keys")
			b.ResetTimer()

			for range b.N {
				result, err := query.AggregateAllVolumes(handle, attrs.Volume, "benchmark", query.AggregateOptions{})
				if err != nil {
					b.Fatalf("aggregating volumes: %v", err)
				}

				benchmarkAggregateResult = result
			}
		})
	}
}

func newBenchmarkStore(b *testing.B) *dal.Store {
	b.Helper()

	meter := noop.NewMeterProvider().Meter("aggregate-benchmark")
	store, err := dal.NewStore(b.TempDir(), logging.NopZap(), meter, dal.DefaultConfig())
	if err != nil {
		b.Fatalf("opening benchmark store: %v", err)
	}
	b.Cleanup(func() { _ = store.Close() })

	return store
}

func seedBenchmarkVolumes(b *testing.B, store *dal.Store, attrs *attributes.Attributes, volumeCount int) {
	b.Helper()

	const batchSize = 10_000

	for start := 0; start < volumeCount; start += batchSize {
		end := min(start+batchSize, volumeCount)
		batch := store.OpenWriteSession()

		for i := start; i < end; i++ {
			asset := fmt.Sprintf("ASSET%02d/2", i%16)
			color := ""
			if i%4 != 0 {
				color = fmt.Sprintf("color-%d", i%4)
			}

			key := domain.VolumeKey{
				AccountKey: domain.AccountKey{
					LedgerName: "benchmark",
					Account:    fmt.Sprintf("account:%08d", i/16),
				},
				Asset: asset,
				Color: color,
			}

			_, err := attrs.Volume.Set(batch, key.Bytes(), &raftcmdpb.VolumePair{
				Input:  commonpb.NewUint256FromUint64(uint64(i + 1)),
				Output: commonpb.NewUint256FromUint64(uint64(i/2 + 1)),
			})
			if err != nil {
				_ = batch.Cancel()

				b.Fatalf("seeding benchmark volume %d: %v", i, err)
			}
		}

		if err := batch.Commit(); err != nil {
			_ = batch.Cancel()

			b.Fatalf("committing benchmark volumes [%d,%d): %v", start, end, err)
		}
	}
}
