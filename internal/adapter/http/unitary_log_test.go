package http

import (
	"fmt"
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

func TestExactlyOneLog(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "l"}},
	}}

	t.Run("single log is returned", func(t *testing.T) {
		t.Parallel()

		got := exactlyOneLog("op", []*commonpb.Log{log}, map[string]any{"ledger": "l"})
		require.Same(t, log, got)
	})

	t.Run("zero logs panic", func(t *testing.T) {
		t.Parallel()

		requirePanicsContaining(t, "op apply did not return exactly one log", func() {
			exactlyOneLog("op", []*commonpb.Log{}, map[string]any{"ledger": "l"})
		})
	})

	t.Run("multiple logs panic", func(t *testing.T) {
		t.Parallel()

		requirePanicsContaining(t, "op apply did not return exactly one log", func() {
			exactlyOneLog("op", []*commonpb.Log{log, log}, map[string]any{"ledger": "l"})
		})
	})

	t.Run("nil sole log panics", func(t *testing.T) {
		t.Parallel()

		requirePanicsContaining(t, "op apply returned a nil log", func() {
			exactlyOneLog("op", []*commonpb.Log{nil}, map[string]any{"ledger": "l"})
		})
	})

	// The details map must survive into the panic value so the jsonRecoverer
	// logs it server-side (assert.Unreachable is a no-op outside Antithesis).
	// operation must be part of it: the Antithesis property message is a
	// shared literal, so details are what tie a failure to its endpoint.
	t.Run("panic value carries diagnostics", func(t *testing.T) {
		t.Parallel()

		defer func() {
			rec := recover()
			require.NotNil(t, rec)

			msg := fmt.Sprint(rec)
			require.Contains(t, msg, "op apply did not return exactly one log")
			require.Contains(t, msg, "ledger:l")
			require.Contains(t, msg, "log_count:2")
			require.Contains(t, msg, "operation:op")
		}()

		exactlyOneLog("op", []*commonpb.Log{log, log}, map[string]any{"ledger": "l"})
	})
}

func TestUnexpectedLogPayload(t *testing.T) {
	t.Parallel()

	log := &commonpb.Log{Sequence: 3, Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{}},
	}}

	got := unexpectedLogPayload("create-transaction", log, map[string]any{"ledger": "l"})

	require.Contains(t, got, "create-transaction apply returned an unexpected log payload type")
	require.Contains(t, got, "ledger:l")
	require.Contains(t, got, "operation:create-transaction")
	require.Contains(t, got, "outer_payload_type:*commonpb.LogPayload_CreateLedger")
}

func TestEmptyLogPayload(t *testing.T) {
	t.Parallel()

	// Correct outer type, but the Apply log carries no inner ledger-log payload.
	log := &commonpb.Log{Sequence: 4, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
		Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: nil},
		}}},
	}}}

	got := emptyLogPayload("create-transaction", log, map[string]any{"ledger": "l"})

	require.Contains(t, got, "create-transaction apply returned a log with no payload body")
	require.Contains(t, got, "ledger:l")
	require.Contains(t, got, "operation:create-transaction")
	require.Contains(t, got, "outer_payload_type:*commonpb.LogPayload_Apply")
}

func TestObservedPayloadDetails(t *testing.T) {
	t.Parallel()

	createLedger := &commonpb.Log{Sequence: 7, Payload: &commonpb.LogPayload{
		Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{}},
	}}
	applyReverted := &commonpb.Log{Sequence: 9, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
		Apply: &commonpb.ApplyLedgerLog{Log: &commonpb.LedgerLog{Data: &commonpb.LedgerLogPayload{
			Payload: &commonpb.LedgerLogPayload_RevertedTransaction{RevertedTransaction: &commonpb.RevertedTransaction{}},
		}}},
	}}}

	cases := []struct {
		name string
		log  *commonpb.Log
		base map[string]any
		want map[string]any
	}{
		{
			name: "non-apply outer payload records only the outer type",
			log:  createLedger,
			base: map[string]any{"ledger": "ledger1"},
			want: map[string]any{
				"ledger":             "ledger1",
				"sequence":           uint64(7),
				"outer_payload_type": "*commonpb.LogPayload_CreateLedger",
			},
		},
		{
			name: "apply outer records the inner type too",
			log:  applyReverted,
			base: map[string]any{"ledger": "ledger1", "transaction_id": "5"},
			want: map[string]any{
				"ledger":             "ledger1",
				"transaction_id":     "5",
				"sequence":           uint64(9),
				"outer_payload_type": "*commonpb.LogPayload_Apply",
				"inner_payload_type": "*commonpb.LedgerLogPayload_RevertedTransaction",
			},
		},
		{
			name: "nil log renders a nil outer type",
			log:  nil,
			base: map[string]any{"ledger": "ledger1"},
			want: map[string]any{
				"ledger":             "ledger1",
				"sequence":           uint64(0),
				"outer_payload_type": "<nil>",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			baseSnapshot := maps.Clone(tc.base)

			got := observedPayloadDetails(tc.log, tc.base)

			require.Equal(t, tc.want, got)
			require.Equal(t, baseSnapshot, tc.base, "base map must not be mutated")
		})
	}
}

func TestMergeDetails(t *testing.T) {
	t.Parallel()

	base := map[string]any{"a": 1, "shared": "base"}
	extra := map[string]any{"b": 2, "shared": "extra"}

	got := mergeDetails(base, extra)

	require.Equal(t, map[string]any{"a": 1, "b": 2, "shared": "extra"}, got, "extra wins on key conflict")
	require.Equal(t, map[string]any{"a": 1, "shared": "base"}, base, "base must not be mutated")
	require.Equal(t, map[string]any{"b": 2, "shared": "extra"}, extra, "extra must not be mutated")
}
