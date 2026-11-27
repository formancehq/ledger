package indexbuilder

import (
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	"github.com/cockroachdb/pebble/v2"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// BenchmarkAccountAssetFoldRepeatedTouch compares the RC.5 first-touch Get
// path with the event writer on an already populated index. One operation is
// a 1,000-log Pebble batch. Each log writes a common ledger-log index row and
// touches the same account/asset twice, exercising in-batch deduplication.
// Five assets and 1,000 accounts yield 5,000 seeded memberships. The benchmark
// includes key construction, committed reads or event writes, and batch commit;
// it does not include query resolution, GC, or unrelated transaction indexes.
func BenchmarkAccountAssetFoldRepeatedTouch(b *testing.B) {
	for _, mode := range []string{"rc5_first_touch_get", "event_append"} {
		b.Run(mode, func(b *testing.B) {
			benchmarkAccountAssetFoldRepeatedTouch(b, mode == "event_append")
		})
	}
}

func benchmarkAccountAssetFoldRepeatedTouch(b *testing.B, events bool) {
	b.Helper()

	const (
		ledger       = "benchmark"
		precision    = uint8(2)
		accountCount = 1_000
	)
	assets := [...]string{"USD", "EUR", "GBP", "JPY", "CHF"}
	accounts := make([]string, accountCount)
	for i := range accounts {
		accounts[i] = fmt.Sprintf("accounts:%04d", i)
	}

	store, err := readstore.New(b.TempDir(), noopLogger{}, readstore.DefaultConfig())
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		if err := store.Close(); err != nil {
			b.Error(err)
		}
	})

	builder := &Builder{readStore: store, kb: dal.NewKeyBuilder(), wb: readstore.NewWriteBatch()}
	seed := store.NewBatch()
	for _, account := range accounts {
		for _, asset := range assets {
			var key []byte
			var value []byte
			if events {
				key = readstore.AccountByAssetEventKey(builder.kb, ledger, asset, precision, account, 1)
			} else {
				key = benchmarkRC5AccountAssetKey(builder.kb, ledger, asset, precision, account)
				value = binary.BigEndian.AppendUint64(nil, 1)
			}
			if err := seed.SetBytes(key, value); err != nil {
				b.Fatal(err)
			}
		}
	}
	if err := seed.Commit(); err != nil {
		b.Fatal(err)
	}
	if err := store.DB().Flush(); err != nil {
		b.Fatal(err)
	}

	const firstSequence = uint64(2)
	nextSequence := firstSequence
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		builder.initBatch(store.NewBatch())
		for range DefaultBatchSize {
			seq := nextSequence
			nextSequence++
			builder.wb.SetEventSequence(seq)
			if err := builder.wb.WriteLedgerLogIndex(builder.kb, ledger, seq, seq); err != nil {
				b.Fatal(err)
			}
			touch := (seq - firstSequence) % uint64(len(accounts)*len(assets))
			account := accounts[touch/uint64(len(assets))]
			asset := assets[touch%uint64(len(assets))]
			for range 2 {
				if events {
					err = builder.writeAccountByAssetDedup(builder.kb, ledger, account, asset, precision)
				} else {
					err = benchmarkRC5AccountAssetTouch(builder, ledger, account, asset, precision)
				}
				if err != nil {
					b.Fatal(err)
				}
			}
		}
		if err := builder.wb.Flush(); err != nil {
			b.Fatal(err)
		}
	}
	b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N*DefaultBatchSize), "ns/log")
}

// benchmarkRC5AccountAssetTouch reproduces the RC.5 committed-state check
// for the populated, no-ledger-delete workload above. Keep this benchmark
// baseline local to the test; production must not regain a point Get here.
func benchmarkRC5AccountAssetTouch(b *Builder, ledger, account, asset string, precision uint8) error {
	key := benchmarkRC5AccountAssetKey(b.kb, ledger, asset, precision, account)
	sk := string(key)
	if _, seen := b.seenAcctAsset[sk]; seen {
		return nil
	}
	b.seenAcctAsset[sk] = struct{}{}

	value, closer, err := b.readStore.DB().Get(key)
	if err == nil {
		stamp := append([]byte(nil), value...)
		_ = closer.Close()
		if len(stamp) == 8 && binary.BigEndian.Uint64(stamp) <= b.wb.EventSequence() {
			return nil
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return err
	}

	return b.wb.Batch().SetBytes(key, binary.BigEndian.AppendUint64(nil, b.wb.EventSequence()))
}

func benchmarkRC5AccountAssetKey(kb *dal.KeyBuilder, ledger, asset string, precision uint8, account string) []byte {
	return kb.Reset().
		PutByte(readstore.PrefixAccountByAsset).
		PutLedgerNameFixed(ledger).
		PutStringNull(asset).
		PutByte(precision).
		PutString(account).
		Build()
}
