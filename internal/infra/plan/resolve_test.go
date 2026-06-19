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
// unmarshal restores a fresh empty proto. Volumes opt out of this default
// via newZeroVolumePair to inject {Input:0, Output:0} instead.
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

// TestNewZeroVolumePair pins the volumes-only seed used by
// resolveAttributePreload when bloom confirms absence: postings on fresh
// accounts must find {Input:0, Output:0} rather than a nil/empty pair.
func TestNewZeroVolumePair(t *testing.T) {
	t.Parallel()

	zero := newZeroVolumePair()
	require.NotNil(t, zero.GetInput())
	require.NotNil(t, zero.GetOutput())
	require.True(t, zero.GetInput().IsZero())
	require.True(t, zero.GetOutput().IsZero())
}
