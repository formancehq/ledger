package processing

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestBornEmpty_MarkAndClearViaUpdate(t *testing.T) {
	t.Parallel()

	ctx := &Context{}

	// A CreatedLedger log marks the ledger born-empty.
	ctx.updateBornEmpty(&commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{
			CreateLedger: &commonpb.CreatedLedgerLog{Name: "l1"},
		},
	})
	require.True(t, ctx.isBornEmpty("l1"))
	require.False(t, ctx.isBornEmpty("other"))

	// A config log (CreateIndex) does NOT clear it.
	ctx.updateBornEmpty(applyLog("l1", &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreateIndex{CreateIndex: &commonpb.CreatedIndexLog{}},
	}))
	require.True(t, ctx.isBornEmpty("l1"))

	// The first indexable data log clears it.
	ctx.updateBornEmpty(applyLog("l1", &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{}},
	}))
	require.False(t, ctx.isBornEmpty("l1"))
}

func TestBornEmpty_NilSafe(t *testing.T) {
	t.Parallel()

	ctx := &Context{}
	require.False(t, ctx.isBornEmpty("x")) // read on nil map
	ctx.updateBornEmpty(applyLog("x", &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: &commonpb.CreatedTransaction{}},
	})) // delete on nil map, must not panic
	require.False(t, ctx.isBornEmpty("x"))
}

func applyLog(ledger string, data *commonpb.LedgerLogPayload) *commonpb.LogPayload {
	return &commonpb.LogPayload{
		Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{
				LedgerName: ledger,
				Log:        &commonpb.LedgerLog{Data: data},
			},
		},
	}
}
