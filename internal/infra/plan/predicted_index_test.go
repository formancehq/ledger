package plan

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// proposalID is set on every fixture so the append lands after at least one
// already-marshalled field rather than onto an empty buffer. Id is field 1 and
// predicted_index is field 5, so vtproto's field-number ordering makes the
// appended bytes the tail of the canonical encoding — which is what lets the
// tests below assert strict byte equality.
const proposalID = uint64(0x1122334455667788)

// TestAppendProposalPredictedIndexMatchesCanonicalMarshal pins the hand-rolled
// append against what the generated marshaller produces for the same message.
//
// AppendProposalPredictedIndex reads the field *number* from the descriptor, so
// a renumbering self-heals. The wire *type* is not covered by that: it is baked
// into the protowireutil.AppendFixed64 call. Changing
// Proposal.predicted_index from fixed64 to uint64 would make the generated
// marshaller emit a varint while this function kept emitting a fixed64 — same
// field number, wrong wire type, decoded as garbage with nothing failing at
// compile time. Comparing against the canonical encoding is what catches it.
func TestAppendProposalPredictedIndexMatchesCanonicalMarshal(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name  string
		index uint64
	}{
		{name: "one", index: 1},
		{name: "small", index: 42},
		{name: "byte boundary", index: 255},
		{name: "two byte boundary", index: 256},
		{name: "large", index: 1 << 40},
		{name: "max", index: ^uint64(0)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base, err := (&raftcmdpb.Proposal{Id: proposalID}).MarshalVT()
			require.NoError(t, err)

			want, err := (&raftcmdpb.Proposal{Id: proposalID, PredictedIndex: tc.index}).MarshalVT()
			require.NoError(t, err)

			got := AppendProposalPredictedIndex(base, tc.index)

			require.Equal(t, want, got,
				"appended encoding must be byte-identical to marshalling the proposal with PredictedIndex set")

			decoded := &raftcmdpb.Proposal{}
			require.NoError(t, proto.Unmarshal(got, decoded))
			require.Equal(t, tc.index, decoded.GetPredictedIndex())
			require.Equal(t, proposalID, decoded.GetId())
			require.True(t, proto.Equal(&raftcmdpb.Proposal{Id: proposalID, PredictedIndex: tc.index}, decoded))
		})
	}
}

// TestAppendProposalPredictedIndexWireFormat asserts the shape of the appended
// bytes directly, so a regression names the wire-type invariant rather than
// only showing a byte-slice diff.
func TestAppendProposalPredictedIndexWireFormat(t *testing.T) {
	t.Parallel()

	const index = uint64(0xDEADBEEFCAFEBABE)

	base, err := (&raftcmdpb.Proposal{Id: proposalID}).MarshalVT()
	require.NoError(t, err)

	suffix := AppendProposalPredictedIndex(base, index)[len(base):]

	num, typ, n := protowire.ConsumeTag(suffix)
	require.Positive(t, n, "appended bytes must start with a valid tag")
	require.Equal(t, predictedIndexField, num, "tag must carry the descriptor's field number")
	require.Equal(t, protowire.Fixed64Type, typ, "predicted_index must be appended as a fixed64")

	value, vn := protowire.ConsumeFixed64(suffix[n:])
	require.Positive(t, vn)
	require.Equal(t, index, value)
	require.Len(t, suffix, n+vn, "append must emit exactly one tag/value pair and nothing else")
}

// TestPredictedIndexFieldIsFixed64 guards the assumption AppendFixed64 encodes.
// It fails the moment predicted_index stops being a fixed64 in raft_cmd.proto,
// pointing at this file instead of letting the mismatch surface as a decoding
// bug at runtime.
func TestPredictedIndexFieldIsFixed64(t *testing.T) {
	t.Parallel()

	fd := (&raftcmdpb.Proposal{}).ProtoReflect().Descriptor().Fields().ByName("predicted_index")
	require.NotNil(t, fd)
	require.Equal(t, protoreflect.Fixed64Kind, fd.Kind(),
		"AppendProposalPredictedIndex hardcodes the fixed64 wire type; update it if the field kind changes")
	require.Equal(t, fd.Number(), predictedIndexField)
}

// TestLookupPredictedIndexField covers both arms of the descriptor lookup that
// seeds predictedIndexField at package init.
//
// The panic arm is the one worth pinning: it fires only if predicted_index is
// dropped from raft_cmd.proto, and a guard that never runs in any test is a
// guard whose message and reachability are unverified. CloseChapterOrder is an
// empty message, so it cannot drift into carrying the field.
func TestLookupPredictedIndexField(t *testing.T) {
	t.Parallel()

	t.Run("field present", func(t *testing.T) {
		t.Parallel()

		md := (&raftcmdpb.Proposal{}).ProtoReflect().Descriptor()

		require.Equal(t, predictedIndexField, lookupPredictedIndexField(md))
	})

	t.Run("field absent panics", func(t *testing.T) {
		t.Parallel()

		md := (&raftcmdpb.CloseChapterOrder{}).ProtoReflect().Descriptor()
		require.Nil(t, md.Fields().ByName("predicted_index"),
			"fixture must be a message without the field, otherwise the guard is not exercised")

		require.PanicsWithValue(t,
			"invariant: "+string(md.FullName())+" has no predicted_index field",
			func() { lookupPredictedIndexField(md) })
	})
}

// TestAppendProposalPredictedIndexZeroIsNoop pins the documented contract: a
// zero index is omitted by proto3, so the append must not emit a second
// occurrence of the field on the wire.
func TestAppendProposalPredictedIndexZeroIsNoop(t *testing.T) {
	t.Parallel()

	base, err := (&raftcmdpb.Proposal{Id: proposalID}).MarshalVT()
	require.NoError(t, err)

	got := AppendProposalPredictedIndex(base, 0)

	require.Equal(t, base, got)

	decoded := &raftcmdpb.Proposal{}
	require.NoError(t, proto.Unmarshal(got, decoded))
	require.Zero(t, decoded.GetPredictedIndex())
}
