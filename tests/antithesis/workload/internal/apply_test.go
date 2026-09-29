package internal_test

import (
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
)

const (
	missingPayload = "CreatedTransaction must carry its transaction payload"
	// The SDK emits nothing for an accepted response, and Capture requires a
	// non-empty stream, so these cases emit a sentinel of their own.
	scenarioRan = "apply extraction scenario ran"
)

func applyResponse(created *commonpb.CreatedTransaction) *servicepb.ApplyResponse {
	ledgerLog := &commonpb.LedgerLog{Id: 1, Data: &commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: created},
	}}

	return &servicepb.ApplyResponse{Logs: []*commonpb.Log{{
		Sequence: 1,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{LedgerName: "default", Log: ledgerLog},
		}},
	}}}
}

// A wrapper whose Transaction is absent must not reach a caller: callers read
// Transaction.Id off the result, where a zero id passes an equality check
// against another zero id.
func TestExtractCreatedTransactionRejectsMissingPayload(t *testing.T) {
	t.Parallel()

	events := sdktest.Capture(t, func() {
		require.Nil(t, internal.ExtractCreatedTransaction(applyResponse(&commonpb.CreatedTransaction{})))
	})
	if events == nil {
		return
	}

	event := sdktest.Find(t, events, missingPayload)
	require.False(t, event.Condition, "a missing payload must be reported as a violation")
}

func TestExtractCreatedTransactionAcceptsPayload(t *testing.T) {
	t.Parallel()

	events := sdktest.Capture(t, func() {
		created := &commonpb.CreatedTransaction{Transaction: &commonpb.Transaction{Id: 7}}
		extracted := internal.ExtractCreatedTransaction(applyResponse(created))
		require.NotNil(t, extracted)
		require.Equal(t, uint64(7), extracted.GetTransaction().GetId())
		assert.Reachable(scenarioRan, nil)
	})
	if events == nil {
		return
	}

	sdktest.Absent(t, events, missingPayload)
}

// An absent CreatedTransaction is the ambiguous error path, not a malformed
// response, so it yields nil without a finding.
func TestExtractCreatedTransactionAllowsAbsentTransaction(t *testing.T) {
	t.Parallel()

	events := sdktest.Capture(t, func() {
		require.Nil(t, internal.ExtractCreatedTransaction(applyResponse(nil)))
		assert.Reachable(scenarioRan, nil)
	})
	if events == nil {
		return
	}

	sdktest.Absent(t, events, missingPayload)
}
