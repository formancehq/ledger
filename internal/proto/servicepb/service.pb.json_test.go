package servicepb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestBulkElement_UnmarshalsSkippableReasons pins the REST opt-in for
// `skippableReasons` at the bulk-entry level. The single-transaction endpoint
// intentionally omits skip support (a unitary caller can catch the 4xx
// directly), so the opt-in surfaces only on bulk entries — decoded here and
// hoisted onto the LedgerApplyRequest by the bulk handler. Accepted values
// are the SHORT reason identifier (matching gRPC ErrorInfo.reason and the
// REST error responses' `errorCode`).
func TestBulkElement_UnmarshalsSkippableReasons(t *testing.T) {
	t.Parallel()

	body := []byte(`{"action":"CREATE_TRANSACTION","data":{"reference":"r"},"skippableReasons":["TRANSACTION_REFERENCE_CONFLICT"]}`)

	var e servicepb.BulkElement
	require.NoError(t, json.Unmarshal(body, &e))
	require.Equal(t,
		[]commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
		e.SkippableReasons,
	)
}

// TestBulkElement_RejectsPrefixedSkippableReason guards against drift: the
// full enum constant "ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT" must NOT
// be silently accepted — clients follow the OpenAPI contract (short form).
func TestBulkElement_RejectsPrefixedSkippableReason(t *testing.T) {
	t.Parallel()

	body := []byte(`{"action":"CREATE_TRANSACTION","data":{"reference":"r"},"skippableReasons":["ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT"]}`)

	var e servicepb.BulkElement
	require.Error(t, json.Unmarshal(body, &e))
}

// TestBulkElement_RejectsUnknownSkippableReason guards against silent typos
// in REST payloads: an unknown enum name must surface as a JSON decode
// error rather than landing on the request as zero/unspecified.
func TestBulkElement_RejectsUnknownSkippableReason(t *testing.T) {
	t.Parallel()

	body := []byte(`{"action":"CREATE_TRANSACTION","data":{"reference":"r"},"skippableReasons":["NOT_A_REAL_REASON"]}`)

	var e servicepb.BulkElement
	require.Error(t, json.Unmarshal(body, &e))
}

// TestBulkElement_NoSkippableReasonsMeansEmpty pins the historical default:
// a bulk entry without the field disables the skip mechanism — the FSM
// observes an empty whitelist and fails fast on any business error.
func TestBulkElement_NoSkippableReasonsMeansEmpty(t *testing.T) {
	t.Parallel()

	body := []byte(`{"action":"CREATE_TRANSACTION","data":{"reference":"r"}}`)

	var e servicepb.BulkElement
	require.NoError(t, json.Unmarshal(body, &e))
	require.Empty(t, e.SkippableReasons)
}

// TestCreateTransactionPayload_IgnoresSkippableReasons pins that the unitary
// payload never carries the field: even if a caller ships `skippableReasons`
// on the JSON body (e.g. copy-paste from a bulk entry), the field is
// silently ignored by the CreateTransactionPayload decoder — the opt-in is
// exclusively a bulk-entry / gRPC-request concern.
func TestCreateTransactionPayload_IgnoresSkippableReasons(t *testing.T) {
	t.Parallel()

	body := []byte(`{"reference":"r","skippableReasons":["TRANSACTION_REFERENCE_CONFLICT"]}`)

	var p servicepb.CreateTransactionPayload
	require.NoError(t, json.Unmarshal(body, &p))
	require.Equal(t, "r", p.GetReference())
}
