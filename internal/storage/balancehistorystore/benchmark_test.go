package balancehistorystore

import (
	"fmt"
	"testing"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain/balancehistory"
)

func BenchmarkHistoryReadByAge(b *testing.B) {
	for _, days := range []int{1, 180, 730} {
		b.Run(fmt.Sprintf("age_days=%d", days), func(b *testing.B) {
			store, err := New(b.TempDir(), logging.NopZap(), DefaultConfig())
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = store.Close() }()

			const accountCount = 100
			effects := make([]balancehistory.Effect, 0, days*accountCount*2)
			for day := 1; day <= days; day++ {
				timestamp := uint64(day) * 24 * 60 * 60 * 1_000_000
				for account := range accountCount {
					effects = append(effects,
						balancehistory.Effect{
							LedgerName: "default", AuditSequence: 1, LogSequence: 1,
							EffectiveAt: timestamp, InsertedAt: timestamp,
							Account: fmt.Sprintf("accounts:%06d", account), AssetBase: "USD", AssetPrecision: 2,
							Input: balancehistory.AmountFromUint64(1),
						},
						balancehistory.Effect{
							LedgerName: "default", AuditSequence: 1, LogSequence: 1,
							EffectiveAt: timestamp, InsertedAt: timestamp,
							Account: "world", AssetBase: "USD", AssetPrecision: 2,
							Output: balancehistory.AmountFromUint64(1),
						},
					)
				}
			}

			if _, err := store.Publish(Publication{
				Effects:  effects,
				Coverage: Coverage{AuditSequence: 1, LogSequence: 1, SourceComplete: true},
			}); err != nil {
				b.Fatal(err)
			}
			view, err := store.OpenView(1)
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = view.Close() }()

			at := uint64(days) * 24 * 60 * 60 * 1_000_000
			accounts := make([]string, accountCount)
			for account := range accountCount {
				accounts[account] = fmt.Sprintf("accounts:%06d", account)
			}

			b.Run("unfiltered", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := view.ReadVolumes("default", TemporalityEffective, at, nil); err != nil {
						b.Fatal(err)
					}
				}
			})

			b.Run("filtered_100", func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					if _, err := view.ReadVolumes("default", TemporalityEffective, at, accounts); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkHistoryReadBySegmentCount(b *testing.B) {
	for _, segmentCount := range []int{1, 4, 16} {
		b.Run(fmt.Sprintf("segments=%d", segmentCount), func(b *testing.B) {
			store, err := New(b.TempDir(), logging.NopZap(), DefaultConfig())
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = store.Close() }()

			for segment := 1; segment <= segmentCount; segment++ {
				effects := make([]balancehistory.Effect, 0, 2_000)
				for account := range 1_000 {
					effects = append(effects,
						balancehistory.Effect{
							LedgerName: "default", AuditSequence: uint64(segment), LogSequence: uint64(segment),
							EffectiveAt: uint64(segment), InsertedAt: uint64(segment),
							Account: fmt.Sprintf("accounts:%06d", account), AssetBase: "USD",
							Input: balancehistory.AmountFromUint64(1),
						},
						balancehistory.Effect{
							LedgerName: "default", AuditSequence: uint64(segment), LogSequence: uint64(segment),
							EffectiveAt: uint64(segment), InsertedAt: uint64(segment),
							Account: "world", AssetBase: "USD",
							Output: balancehistory.AmountFromUint64(1),
						},
					)
				}
				if _, err := store.Publish(Publication{
					Effects:  effects,
					Coverage: Coverage{AuditSequence: uint64(segment), LogSequence: uint64(segment), SourceComplete: true},
				}); err != nil {
					b.Fatal(err)
				}
			}

			view, err := store.OpenView(uint64(segmentCount))
			if err != nil {
				b.Fatal(err)
			}
			defer func() { _ = view.Close() }()

			b.ReportAllocs()
			for b.Loop() {
				if _, err := view.ReadVolumes("default", TemporalityEffective, uint64(segmentCount), nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
