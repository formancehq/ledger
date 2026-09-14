package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func skipRequest(req *servicepb.Request, reason commonpb.ErrorReason) *servicepb.Request {
	req.GetApply().SkippableReasons = []commonpb.ErrorReason{reason}

	return req
}

func TestApplySkipsReferenceConflictAndContinues(t *testing.T) {
	t.Parallel()
	first := NewGlobalState().Apply(bulkOf(oracletest.TxReqRefL("L", "ref", "world", "a", "USD", 5)))
	require.True(t, first.OK)
	before := first.State.Fingerprint()
	batch := keyedBulk("skip", skipRequest(oracletest.TxReqRefL("L", "ref", "world", "a", "USD", 99), commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT), oracletest.TxReq("world", "a", "USD", 2))
	got := first.State.Apply(batch)
	require.True(t, got.OK, got.Reason)
	require.Equal(t, before, first.State.Fingerprint(), "fork must not mutate its predecessor")
	rows := got.State.Ledger("L").LogRows()
	require.Len(t, rows, 3)
	require.Equal(t, "order_skipped", rows[1].Kind)
	require.Equal(t, uint64(2), got.Orders[1].TxID)
	volume, ok := got.State.Ledger("L").Volumes().Get(VolumeKey{Address: "a", Asset: "USD"})
	require.True(t, ok)
	require.Equal(t, "7", volume.Input.Dec())
	replay := got.State.Apply(batch)
	require.True(t, replay.OK)
	require.Equal(t, got.State.Fingerprint(), replay.State.Fingerprint())
	require.Equal(t, got.Orders, replay.Orders)
}

func TestApplySkippableAdmissionPrecedesBusinessFailure(t *testing.T) {
	t.Parallel()
	for _, reason := range []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_UNSPECIFIED, commonpb.ErrorReason_ERROR_REASON_ACCOUNT_TYPE_NOT_FOUND} {
		t.Run(reason.String(), func(t *testing.T) {
			t.Parallel()
			bad := skipRequest(oracletest.TxReq("world", "a", "USD", 1), reason)
			base := NewGlobalState()
			got := base.Apply(bulkOf(oracletest.RemoveTypeReq("absent"), bad))
			require.False(t, got.OK)
			require.Equal(t, "VALIDATION", got.Reason)
			require.Equal(t, base.Fingerprint(), got.State.Fingerprint())
		})
	}
}

func TestApplySkippedOrderStillRollsBackWithLaterFailure(t *testing.T) {
	t.Parallel()
	first := NewGlobalState().Apply(bulkOf(oracletest.TxReqRefL("L", "ref", "world", "a", "USD", 5)))
	require.True(t, first.OK)
	got := first.State.Apply(bulkOf(
		oracletest.TxReq("world", "a", "USD", 3),
		skipRequest(oracletest.TxReqRefL("L", "ref", "world", "a", "USD", 99), commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT),
		oracletest.RemoveTypeReq("absent"),
	))
	require.False(t, got.OK)
	require.Equal(t, "ACCOUNT_TYPE_NOT_FOUND", got.Reason)
	require.Equal(t, first.State.Fingerprint(), got.State.Fingerprint())
	require.Len(t, got.State.Ledger("L").LogRows(), 1)
}

func TestApplySkippableOptInDoesNotSkipAnotherFailure(t *testing.T) {
	t.Parallel()
	base := NewGlobalState()
	got := base.Apply(bulkOf(skipRequest(oracletest.TxReq("a", "b", "USD", 3), commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)))
	require.False(t, got.OK)
	require.Equal(t, "INSUFFICIENT_FUNDS", got.Reason)
	require.Equal(t, base.Fingerprint(), got.State.Fingerprint())
}

func TestSkippedLogPayloadPinsReasonAndCorrelators(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().Apply(bulkOf(oracletest.TxReqRefL("L", "ref", "world", "a", "USD", 5))).State
	result := base.Apply(bulkOf(skipRequest(oracletest.TxReqRefL("L", "ref", "world", "a", "USD", 1), commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT)))
	require.True(t, result.OK)
	expected := result.State.Ledger("L").LogRows()[1]
	served := &commonpb.LedgerLogPayload{Payload: &commonpb.LedgerLogPayload_OrderSkipped{OrderSkipped: &commonpb.OrderSkippedLog{
		Reason:  commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		Context: map[string]string{"ledger": "L", "reference": "ref", "existingTransactionId": "1"},
	}}}
	require.Equal(t, expected.Payload, CanonicalServedLogPayload(served))
	served.GetOrderSkipped().Context["existingTransactionId"] = "2"
	require.NotEqual(t, expected.Payload, CanonicalServedLogPayload(served))
	served.GetOrderSkipped().Context["existingTransactionId"] = "1"
	served.GetOrderSkipped().Reason = commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND
	require.NotEqual(t, expected.Payload, CanonicalServedLogPayload(served))
}
