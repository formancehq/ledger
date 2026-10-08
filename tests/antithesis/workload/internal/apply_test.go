package internal_test

import (
	"testing"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/stretchr/testify/require"

	ledgerpb "github.com/formancehq/ledger/pkg/client/v3/grpc"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/sdktest"
)

const (
	missingPayload = "CreatedTransaction must carry its transaction payload"
	// The SDK emits nothing for an accepted response, and Capture requires a
	// non-empty stream, so these cases emit a sentinel of their own.
	scenarioRan = "apply extraction scenario ran"
)

func applyLog(created *ledgerpb.CreatedTransaction) *ledgerpb.Log {
	ledgerLog := &ledgerpb.LedgerLog{Id: 1, Data: &ledgerpb.LedgerLogPayload{
		Payload: &ledgerpb.LedgerLogPayload_CreatedTransaction{CreatedTransaction: created},
	}}

	return &ledgerpb.Log{
		Sequence: 1,
		Payload: &ledgerpb.LogPayload{Type: &ledgerpb.LogPayload_Apply{
			Apply: &ledgerpb.ApplyLedgerLog{LedgerName: "default", Log: ledgerLog},
		}},
	}
}

func applyResponse(created *ledgerpb.CreatedTransaction) *ledgerpb.ApplyResponse {
	return &ledgerpb.ApplyResponse{Logs: []*ledgerpb.Log{applyLog(created)}}
}

// A wrapper whose Transaction is absent must not reach a caller: callers read
// Transaction.Id off the result, where a zero id passes an equality check
// against another zero id.
func TestExtractCreatedTransactionRejectsMissingPayload(t *testing.T) {
	t.Parallel()

	events := sdktest.Capture(t, func() {
		require.Nil(t, internal.ExtractCreatedTransaction(applyResponse(&ledgerpb.CreatedTransaction{})))
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
		created := &ledgerpb.CreatedTransaction{Transaction: &ledgerpb.Transaction{Id: 7}}
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

// The bulk drivers select one log out of many and must not reach a malformed
// payload by skipping the response-level entry point.
func TestCreatedTransactionFromLogRejectsMissingPayload(t *testing.T) {
	t.Parallel()

	events := sdktest.Capture(t, func() {
		require.Nil(t, internal.CreatedTransactionFromLog(applyLog(&ledgerpb.CreatedTransaction{})))
	})
	if events == nil {
		return
	}

	event := sdktest.Find(t, events, missingPayload)
	require.False(t, event.Condition, "a missing payload must be reported as a violation")
}
