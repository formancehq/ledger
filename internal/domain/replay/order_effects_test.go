package replay

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// mirrorIngestOrder wraps a prebuilt entry: MirrorLogEntry.Data has an
// unexported oneof interface type, so the entry is built by the caller rather
// than passed in as a bare payload.
func mirrorIngestOrder(ledger string, entry *raftcmdpb.MirrorLogEntry) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_MirrorIngest{
					MirrorIngest: &raftcmdpb.MirrorIngestOrder{Entry: entry},
				},
			},
		},
	}
}

// applyCreateTransactionOrder builds a non-mirror CreateTransaction order.
func applyCreateTransactionOrder(ledger string, ct *raftcmdpb.CreateTransactionOrder) *raftcmdpb.Order {
	return &raftcmdpb.Order{
		Type: &raftcmdpb.Order_LedgerScoped{
			LedgerScoped: &raftcmdpb.LedgerScopedOrder{
				Ledger: ledger,
				Payload: &raftcmdpb.LedgerScopedOrder_Apply{
					Apply: &raftcmdpb.LedgerApplyOrder{
						Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{CreateTransaction: ct},
					},
				},
			},
		},
	}
}

func TestDecodeOrderEffects(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		order *raftcmdpb.Order
		want  OrderEffects
	}{
		{
			name:  "system-scoped order has no boundary effect",
			order: &raftcmdpb.Order{},
			want:  OrderEffects{},
		},
		{
			// The fill-gap kind is the only one whose source id also reaches
			// the ledger-log stream, via FilledGapLog.original_id.
			name: "mirror fill-gap carries skipped ids and the source id",
			order: mirrorIngestOrder("led", &raftcmdpb.MirrorLogEntry{
				V2LogId: 7,
				Data: &raftcmdpb.MirrorLogEntry_FillGap{
					FillGap: &raftcmdpb.MirrorFillGap{SkippedTransactionIds: []uint64{5, 9}},
				},
			}),
			want: OrderEffects{
				Ledger:                "led",
				SkippedTransactionIDs: []uint64{5, 9},
				MirrorV2LogID:         7,
			},
		},
		{
			// The regression EN-1776 turned on: this kind has no fill-gap, so
			// the old guard returned the zero value and the ledger name was
			// lost along with the source id.
			name: "mirror created-transaction carries the source id",
			order: mirrorIngestOrder("led", &raftcmdpb.MirrorLogEntry{
				V2LogId: 42,
				Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
					CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{TransactionId: 1},
				},
			}),
			want: OrderEffects{Ledger: "led", MirrorV2LogID: 42},
		},
		{
			name: "mirror saved-metadata carries the source id",
			order: mirrorIngestOrder("led", &raftcmdpb.MirrorLogEntry{
				V2LogId: 8,
				Data: &raftcmdpb.MirrorLogEntry_SavedMetadata{
					SavedMetadata: &raftcmdpb.MirrorSavedMetadata{},
				},
			}),
			want: OrderEffects{Ledger: "led", MirrorV2LogID: 8},
		},
		{
			// v2 log ids are 1-based, so zero is malformed rather than a
			// legitimate position — processMirrorIngest rejects it outright and
			// never records it, so it must not be folded here either.
			name: "mirror ingest with a zero source id has no effect",
			order: mirrorIngestOrder("led", &raftcmdpb.MirrorLogEntry{
				V2LogId: 0,
				Data: &raftcmdpb.MirrorLogEntry_CreatedTransaction{
					CreatedTransaction: &raftcmdpb.MirrorCreatedTransaction{TransactionId: 1},
				},
			}),
			want: OrderEffects{},
		},
		{
			name:  "postings-sourced transaction has no boundary effect",
			order: applyCreateTransactionOrder("led", &raftcmdpb.CreateTransactionOrder{}),
			want:  OrderEffects{},
		},
		{
			// A script-sourced transaction is a plain apply order: it advances
			// no boundary field the ledger-log stream does not already carry,
			// so it drops out here like the postings-sourced case above. This
			// pins that the guard keys off the mirror source id and the fill
			// gap, not off how the transaction content was expressed.
			name: "script-sourced transaction has no boundary effect",
			order: applyCreateTransactionOrder("led", &raftcmdpb.CreateTransactionOrder{
				Script: &commonpb.Script{Plain: "send [USD/2 1] (source = @a destination = @b)"},
			}),
			want: OrderEffects{},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			raw, err := tc.order.MarshalVT()
			require.NoError(t, err)

			got, err := DecodeOrderEffects(raw)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestDecodeOrderEffects_RejectsMalformedBytes(t *testing.T) {
	t.Parallel()

	_, err := DecodeOrderEffects([]byte{0xff, 0xff, 0xff})
	require.Error(t, err)
}
