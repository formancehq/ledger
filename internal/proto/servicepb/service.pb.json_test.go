package servicepb_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
)

// TestCreateTransactionPayload_UnmarshalsSkippableReasons pins the REST opt-in
// for `skippableReasons`. The default protoc-gen-go tag is snake_case, so a
// plain decode would drop the camelCase key the OpenAPI contract advertises
// and the FSM would observe an empty whitelist regardless of what the
// caller submitted. The accepted value is the SHORT reason identifier (the
// same string the gRPC ErrorInfo.reason and the REST error responses use),
// matching what openapi.yml documents.
func TestCreateTransactionPayload_UnmarshalsSkippableReasons(t *testing.T) {
	t.Parallel()

	body := []byte(`{"reference":"r","skippableReasons":["TRANSACTION_REFERENCE_CONFLICT"]}`)

	var p servicepb.CreateTransactionPayload
	require.NoError(t, json.Unmarshal(body, &p))
	require.Equal(t,
		[]commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT},
		p.GetSkippableReasons(),
	)
}

// TestCreateTransactionPayload_MarshalsSkippableReasons mirrors the decode
// path: the Go side must emit the SHORT reason names (matching what the
// rest of the REST/gRPC error surface uses) so the JSON shape lines up
// with openapi.yml.
func TestCreateTransactionPayload_MarshalsSkippableReasons(t *testing.T) {
	t.Parallel()

	p := &servicepb.CreateTransactionPayload{
		Reference: "r",
		SkippableReasons: []commonpb.ErrorReason{
			commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT,
		},
	}

	encoded, err := json.Marshal(p)
	require.NoError(t, err)

	var round map[string]any
	require.NoError(t, json.Unmarshal(encoded, &round))
	require.Equal(t, []any{"TRANSACTION_REFERENCE_CONFLICT"}, round["skippableReasons"])
}

// TestCreateTransactionPayload_RejectsPrefixedSkippableReason guards
// against accidental drift: the full enum constant
// "ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT" must NOT be silently
// accepted — clients should follow the OpenAPI contract (short form).
func TestCreateTransactionPayload_RejectsPrefixedSkippableReason(t *testing.T) {
	t.Parallel()

	body := []byte(`{"reference":"r","skippableReasons":["ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT"]}`)

	var p servicepb.CreateTransactionPayload
	require.Error(t, json.Unmarshal(body, &p))
}

// TestCreateTransactionPayload_RejectsUnknownSkippableReason guards against
// silent typos in REST payloads: an unknown enum name must surface as a
// JSON decode error rather than landing on the order as zero/unspecified.
func TestCreateTransactionPayload_RejectsUnknownSkippableReason(t *testing.T) {
	t.Parallel()

	body := []byte(`{"reference":"r","skippableReasons":["NOT_A_REAL_REASON"]}`)

	var p servicepb.CreateTransactionPayload
	require.Error(t, json.Unmarshal(body, &p))
}

// TestCreateTransactionPayload_OmitsEmptySkippableReasons asserts the
// `omitempty` behaviour: a payload with no opt-in must not advertise the
// field on the wire (clients that did not set it should not see it).
func TestCreateTransactionPayload_OmitsEmptySkippableReasons(t *testing.T) {
	t.Parallel()

	encoded, err := json.Marshal(&servicepb.CreateTransactionPayload{Reference: "r"})
	require.NoError(t, err)

	var round map[string]any
	require.NoError(t, json.Unmarshal(encoded, &round))
	_, hasField := round["skippableReasons"]
	require.False(t, hasField, "skippableReasons should be omitted when empty")
}
