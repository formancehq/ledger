package state

import (
	"errors"
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestErrCoverageMiss_Describable pins the Describable contract: the
// runtime wires *ErrCoverageMiss into a domain.BusinessError, so its
// Kind / Reason / Metadata must surface stable values consumed by
// adapters at the API edge.
func TestErrCoverageMiss_Describable(t *testing.T) {
	t.Parallel()

	miss := &ErrCoverageMiss{
		Attribute:    "ledgers",
		CanonicalHex: "deadbeef",
		IDHex:        "0102",
		RaftIndex:    42,
	}

	require.Equal(t, domain.KindInternal, domain.Kind(miss))
	require.Equal(t, domain.ErrReasonCoverageMiss, miss.Reason())

	md := miss.Metadata()
	require.Equal(t, "ledgers", md["attribute"])
	require.Equal(t, "deadbeef", md["canonicalHex"])
	require.Equal(t, "0102", md["idHex"])
	require.Equal(t, strconv.FormatUint(42, 10), md["raftIndex"])

	require.Contains(t, miss.Error(), "ledgers")
	require.Contains(t, miss.Error(), "0102")
	require.Contains(t, miss.Error(), "42")
}

// TestErrCoverageMissSurvivesStoreFailure pins the EN-1379 propagation
// contract against the REAL *ErrCoverageMiss type. domain.StoreFailure matches
// on the Reason string (internal/domain/processing cannot import this package
// — import cycle), so this is the test that proves the string contract and the
// concrete type actually agree.
//
// It matters because buildAuditFailure reads Reason()/Metadata() off the
// OUTERMOST Describable and never unwraps: if StoreFailure wrapped the miss,
// the hash-chained audit entry would permanently record
// STORAGE_OPERATION_FAILED with the identifying key stripped.
func TestErrCoverageMissSurvivesStoreFailure(t *testing.T) {
	t.Parallel()

	miss := &ErrCoverageMiss{
		Attribute:    "volumes",
		CanonicalHex: "deadbeef",
		IDHex:        "0102",
		RaftIndex:    42,
	}

	tests := []struct {
		name string
		err  error
	}{
		{name: "bare", err: miss},
		{name: "wrapped once", err: fmt.Errorf("numscript: %w", miss)},
		{name: "wrapped in a storage operation", err: &domain.ErrStorageOperation{Operation: "loading volume", Cause: miss}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := domain.StoreFailure("loading volume", tt.err)

			require.Same(t, miss, got)
			require.Equal(t, domain.ErrReasonCoverageMiss, got.Reason())

			md := got.Metadata()
			require.Equal(t, "volumes", md["attribute"])
			require.Equal(t, "deadbeef", md["canonicalHex"])
			require.Equal(t, "0102", md["idHex"])
			require.Equal(t, "42", md["raftIndex"])
		})
	}
}

// TestGenuineStoreFaultStillWraps guards the other direction: a real IO failure
// must keep the storage label, so the EN-1379 branch cannot silently
// reclassify infrastructure faults as admission bugs.
func TestGenuineStoreFaultStillWraps(t *testing.T) {
	t.Parallel()

	cause := errors.New("pebble: db closed")

	got := domain.StoreFailure("loading volume", cause)

	require.Equal(t, domain.ErrReasonStorageOperation, got.Reason())
	require.ErrorIs(t, got, cause)
}

// TestKindLabel exhaustively pins the (sub-attribute byte → label)
// mapping the gatedScope counter + structured log rely on. A new
// SubAttrXxx that forgets to land here would surface as
// "unknown(0xNN)" in metrics and logs; this test fails loudly if the
// table drifts.
func TestKindLabel(t *testing.T) {
	t.Parallel()

	cases := []struct {
		kind  byte
		label string
	}{
		{dal.SubAttrVolume, "volumes"},
		{dal.SubAttrMetadata, "account_metadata"},
		{dal.SubAttrReference, "references"},
		{dal.SubAttrLedger, "ledgers"},
		{dal.SubAttrBoundary, "boundaries"},
		{dal.SubAttrSinkConfig, "sink_configs"},
		{dal.SubAttrNumscriptVersion, "numscript_versions"},
		{dal.SubAttrTransaction, "transactions"},
		{dal.SubAttrNumscriptContent, "numscript_contents"},
		{dal.SubAttrPreparedQuery, "prepared_queries"},
		{dal.SubAttrLedgerMetadata, "ledger_metadata"},
	}

	for _, c := range cases {
		require.Equal(t, c.label, kindLabel(c.kind))
	}

	require.Equal(t, "unknown(0xff)", kindLabel(0xff))
}

// TestApplyPlans_Errors pins the *domain.ErrInvalidExecutionPlan paths
// that applyPlans (and indirectly NewScope) surface to the FSM: a
// coverage bit pointing past the plans slice, and a plan declaring an
// attr_code the FSM doesn't handle. Both must propagate cleanly rather
// than silently dropping the bit.
func TestApplyPlans_Errors(t *testing.T) {
	t.Parallel()

	t.Run("coverage bit past plans length", func(t *testing.T) {
		t.Parallel()

		var coverage coverageSlots

		err := applyPlans(&coverage, []*raftcmdpb.AttributeCoverage{}, []byte{0b1})
		require.NotNil(t, err)
		require.Contains(t, err.Reason_, "coverage_bits flags position 0 past plans length 0")
	})

	t.Run("plan declares unknown attr_code (proposal-wide)", func(t *testing.T) {
		t.Parallel()

		var coverage coverageSlots

		u128, _ := attributes.MakeKey(domain.LedgerKey{Name: "L"}.Bytes())
		plans := []*raftcmdpb.AttributeCoverage{{
			Id:       &raftcmdpb.AttributeID{Id: u128[:]},
			AttrCode: 0xff, // not in cacheAttrKinds
		}}

		// applyAllPlans (the proposal-wide path used by NewProposalScope)
		// walks every plan and must surface the unknown attr_code.
		err := applyAllPlans(&coverage, plans)
		require.NotNil(t, err)
		require.Contains(t, err.Reason_, "0xff")
		require.Contains(t, err.Reason_, "FSM does not handle")
	})

	t.Run("coverage bit on plan with unknown attr_code", func(t *testing.T) {
		t.Parallel()

		var coverage coverageSlots

		u128, _ := attributes.MakeKey(domain.LedgerKey{Name: "L"}.Bytes())
		plans := []*raftcmdpb.AttributeCoverage{{
			Id:       &raftcmdpb.AttributeID{Id: u128[:]},
			AttrCode: 0xfe,
		}}

		err := applyPlans(&coverage, plans, []byte{0b1})
		require.NotNil(t, err)
		require.Contains(t, err.Reason_, "plans[0]")
		require.Contains(t, err.Reason_, "0xfe")
	})

	t.Run("plan with nil AttributeID", func(t *testing.T) {
		t.Parallel()

		var coverage coverageSlots

		plans := []*raftcmdpb.AttributeCoverage{{
			Id:       nil, // forged / decoded-incomplete envelope
			AttrCode: uint32(dal.SubAttrLedger),
		}}

		err := applyPlans(&coverage, plans, []byte{0b1})
		require.NotNil(t, err)
		require.Contains(t, err.Reason_, "plans[0]")
		require.Contains(t, err.Reason_, "16-byte AttributeID")
	})

	t.Run("plan with short AttributeID payload", func(t *testing.T) {
		t.Parallel()

		var coverage coverageSlots

		plans := []*raftcmdpb.AttributeCoverage{{
			Id:       &raftcmdpb.AttributeID{Id: []byte{0x01, 0x02}}, // 2 bytes — would silently zero-pad
			AttrCode: uint32(dal.SubAttrLedger),
		}}

		err := applyAllPlans(&coverage, plans)
		require.NotNil(t, err)
		require.Contains(t, err.Reason_, "plans[0]")
		require.Contains(t, err.Reason_, "16-byte AttributeID")
		require.Contains(t, err.Reason_, "got 2 bytes")
	})
}

// TestApplyAllPlans_AcceptsValidPlans pins the contract that makes the
// post-Preload NewProposalScope branch in applyProposal unreachable: any
// AttributeCoverage set that passes validatePlan (16-byte AttributeID +
// an attr_code the FSM handles) is accepted by applyAllPlans without error.
// Preload runs the SAME validatePlan over the SAME immutable plan before
// order processing, so a NewProposalScope failure after a successful
// Preload can only mean the two checks diverged — an FSM invariant, not a
// business outcome.
func TestApplyAllPlans_AcceptsValidPlans(t *testing.T) {
	t.Parallel()

	var coverage coverageSlots

	ledgerU128, _ := attributes.MakeKey(domain.LedgerKey{Name: "L"}.Bytes())
	volumeU128, _ := attributes.MakeKey(domain.LedgerKey{Name: "L2"}.Bytes())

	plans := []*raftcmdpb.AttributeCoverage{
		{
			Id:       &raftcmdpb.AttributeID{Id: ledgerU128[:]},
			AttrCode: uint32(dal.SubAttrLedger),
		},
		{
			Id:       &raftcmdpb.AttributeID{Id: volumeU128[:]},
			AttrCode: uint32(dal.SubAttrVolume),
		},
	}

	// Each plan must first pass the shared envelope check.
	for i, p := range plans {
		require.Nil(t, validatePlan(p, i))
	}

	// applyAllPlans (the proposal-wide path NewProposalScope uses) accepts
	// every validatePlan-passing plan.
	require.Nil(t, applyAllPlans(&coverage, plans))
}

// TestPlanInvariantDescribable pins the predicate that decides whether
// an error coming out of applyTechnicalUpdates / NewScope should be
// surfaced as a business-level ApplyResult.Error (admission bug) or
// propagated as an FSM-killing error (storage failure, etc.).
func TestPlanInvariantDescribable(t *testing.T) {
	t.Parallel()

	require.Nil(t, planInvariantDescribable(nil),
		"nil error must yield no Describable")

	require.Nil(t, planInvariantDescribable(errors.New("pebble write failed")),
		"unrelated errors must not look like plan invariants")

	miss := &ErrCoverageMiss{Attribute: "ledgers"}
	require.Equal(t, miss, planInvariantDescribable(miss),
		"ErrCoverageMiss must be returned as-is")

	wrappedMiss := fmt.Errorf("applying technical_updates[0]: %w", miss)
	require.Equal(t, miss, planInvariantDescribable(wrappedMiss),
		"errors.As must unwrap through the FSM dispatch wrapper")

	invalid := &domain.ErrInvalidExecutionPlan{Reason_: "bit 7 past plans length 3"}
	require.Equal(t, invalid, planInvariantDescribable(invalid),
		"ErrInvalidExecutionPlan must be returned as-is")
}

// TestLsbIndex covers every single-bit byte (the only values the
// caller iterates) and the all-zero fallback.
func TestLsbIndex(t *testing.T) {
	t.Parallel()

	for i := range 8 {
		require.Equal(t, i, lsbIndex(byte(1<<i)))
	}

	require.Equal(t, 0, lsbIndex(0))
}
