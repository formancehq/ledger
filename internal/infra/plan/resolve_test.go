package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestBuildPreloadPayload_RoundTrip pins the wire contract of the generic
// builder: marshal a typed proto into Preload.raw_value, unmarshal it back
// into a fresh instance of the same type, and assert equality on a field
// the test caller controls.
func TestBuildPreloadPayload_RoundTrip(t *testing.T) {
	t.Parallel()

	source := &commonpb.TransactionReferenceValue{TransactionId: 123}

	attrValue, err := buildPreloadPayload(dal.SubAttrReference, source)
	require.NoError(t, err)

	got := &commonpb.TransactionReferenceValue{}
	require.NoError(t, got.UnmarshalVT(attrValue.GetRawValue()))
	require.Equal(t, source.GetTransactionId(), got.GetTransactionId())
}

// TestBuildPreloadPayload_NilValue confirms the builder accepts a nil-zero
// pointer — vtproto marshals a typed nil to empty bytes, the FSM-side
// unmarshal restores a fresh empty proto. After EN-1378 every attribute
// (Volume included) follows this default and Declare-on-absent is the
// uniform pattern; no attribute injects a typed zero into the plan anymore.
func TestBuildPreloadPayload_NilValue(t *testing.T) {
	t.Parallel()

	var nilRef *commonpb.TransactionReferenceValue

	attrValue, err := buildPreloadPayload(dal.SubAttrReference, nilRef)
	require.NoError(t, err)
	require.Empty(t, attrValue.GetRawValue())

	got := &commonpb.TransactionReferenceValue{}
	require.NoError(t, got.UnmarshalVT(attrValue.GetRawValue()))
	require.Equal(t, uint64(0), got.GetTransactionId())
}
