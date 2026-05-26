package state

import (
	"fmt"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/proto/auditpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

// makeRealisticOrder creates a CreateTransaction order with one posting
// (users:XXXXXX -> merchants:shop-42, EUR/2).
func makeRealisticOrder(i int) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_Apply{
			Apply: &raftcmdpb.LedgerApplyOrder{
				Ledger: "bench-ledger",
				Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{
					CreateTransaction: &raftcmdpb.CreateTransactionOrder{
						Postings: []*commonpb.Posting{
							{
								Source:      fmt.Sprintf("users:%06d", i),
								Destination: "merchants:shop-42",
								Amount:      commonpb.NewUint256FromUint64(100),
								Asset:       "EUR/2",
							},
						},
						Force: true,
					},
				},
			},
		},
	}
}

func BenchmarkAuditWrite(b *testing.B) {
	batchSizes := []int{1, 5, 10, 50, 100, 500}

	for _, n := range batchSizes {
		orders := make([]*raftcmdpb.Order, n)
		for i := range n {
			orders[i] = makeRealisticOrder(i)
		}

		items := make([]*auditpb.AuditItem, n)
		for i, order := range orders {
			items[i] = &auditpb.AuditItem{
				OrderIndex:  uint32(i),
				Order:       order,
				LogSequence: uint64(i + 1),
			}
		}

		b.Run(fmt.Sprintf("Monolithic/n=%d", n), func(b *testing.B) {
			ctx := logging.TestingContext()
			logger := logging.FromContext(ctx)
			meter := noop.NewMeterProvider().Meter("bench")

			store, err := dal.NewStore(b.TempDir(), logger, meter, dal.DefaultConfig())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })

			seq := uint64(0)

			for b.Loop() {
				seq++

				entry := &auditpb.AuditEntry{
					Sequence:   seq,
					ProposalId: seq,
					OrderCount: uint32(n),
					Items:      items,
					Ledgers:    []string{"bench-ledger"},
					Outcome: &auditpb.AuditEntry_Success{
						Success: &auditpb.AuditSuccess{
							MinLogSequence: 1,
							MaxLogSequence: uint64(n),
						},
					},
				}

				batch := store.NewBatch()

				if batchErr := appendAuditEntries(batch, entry); batchErr != nil {
					b.Fatal(batchErr)
				}

				if commitErr := batch.Commit(); commitErr != nil {
					b.Fatal(commitErr)
				}
			}
		})

		b.Run(fmt.Sprintf("Split/n=%d", n), func(b *testing.B) {
			ctx := logging.TestingContext()
			logger := logging.FromContext(ctx)
			meter := noop.NewMeterProvider().Meter("bench")

			store, err := dal.NewStore(b.TempDir(), logger, meter, dal.DefaultConfig())
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { _ = store.Close() })

			seq := uint64(0)

			for b.Loop() {
				seq++

				header := &auditpb.AuditEntry{
					Sequence:   seq,
					ProposalId: seq,
					OrderCount: uint32(n),
					Ledgers:    []string{"bench-ledger"},
					Outcome: &auditpb.AuditEntry_Success{
						Success: &auditpb.AuditSuccess{
							MinLogSequence: 1,
							MaxLogSequence: uint64(n),
						},
					},
				}

				batch := store.NewBatch()

				if batchErr := appendAuditEntries(batch, header); batchErr != nil {
					b.Fatal(batchErr)
				}

				if batchErr := appendAuditItems(batch, seq, items...); batchErr != nil {
					b.Fatal(batchErr)
				}

				if commitErr := batch.Commit(); commitErr != nil {
					b.Fatal(commitErr)
				}
			}
		})
	}
}
