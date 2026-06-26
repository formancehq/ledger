package plan

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// TestBitsForNeeds_SameCanonicalDifferentAttrCode pins the regression
// behind the gaming/lending/marketplace scenario panics: a Needs that
// declares both Ledgers["gaming"] and Boundaries["gaming"] shares a
// canonical (the raw name) and therefore the same U128. Indexing plans
// by U128 alone collapsed the two plans onto a single bitset position;
// the Ledgers need ended up flipping the Boundaries bit and the FSM-side
// Plan.Ledgers sub-reader crashed on the first GetLedger probe.
//
// The fix indexes by (U128, attrCode) so each plan keeps its own bit.
func TestBitsForNeeds_SameCanonicalDifferentAttrCode(t *testing.T) {
	t.Parallel()

	const ledgerName = "gaming"
	canonical := domain.LedgerKey{Name: ledgerName}.Bytes()
	u128, _ := attributes.MakeKey(canonical)

	plans := []*raftcmdpb.AttributePlan{
		{
			Id: &raftcmdpb.AttributeID{Id: u128[:]}, AttrCode: uint32(dal.SubAttrLedger),
			Intent: &raftcmdpb.AttributePlan_Declare{Declare: &raftcmdpb.Declare{}},
		},
		{
			Id: &raftcmdpb.AttributeID{Id: u128[:]}, AttrCode: uint32(dal.SubAttrBoundary),
			Intent: &raftcmdpb.AttributePlan_Declare{Declare: &raftcmdpb.Declare{}},
		},
	}

	needs := NewNeeds()
	needs.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}
	needs.Boundaries[domain.LedgerKey{Name: ledgerName}] = struct{}{}

	require.Equal(t, []byte{0b11}, bitsForNeeds(needs, plans),
		"both Ledger (bit 0) and Boundary (bit 1) plans must be flagged even though they share a U128")
}

// TestBitsForNeeds_CoversEveryNeedsKind pins setIDInBitset's
// exhaustive dispatch over every Needs map. A new field on Needs that
// forgets a mark() arm here would silently never flag its bit in the
// coverage map, so an FSM read against that kind would surface as
// ErrCoverageMiss even when admission declared it.
func TestBitsForNeeds_CoversEveryNeedsKind(t *testing.T) {
	t.Parallel()

	type kindCase struct {
		attrCode  byte
		canonical []byte
		add       func(*Needs)
	}

	cases := []kindCase{
		{
			attrCode:  dal.SubAttrLedger,
			canonical: domain.LedgerKey{Name: "L"}.Bytes(),
			add:       func(n *Needs) { n.Ledgers[domain.LedgerKey{Name: "L"}] = struct{}{} },
		},
		{
			attrCode:  dal.SubAttrBoundary,
			canonical: domain.LedgerKey{Name: "L"}.Bytes(),
			add:       func(n *Needs) { n.Boundaries[domain.LedgerKey{Name: "L"}] = struct{}{} },
		},
		{
			attrCode:  dal.SubAttrVolume,
			canonical: domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "a"}, Asset: "USD"}.Bytes(),
			add: func(n *Needs) {
				n.Volumes[domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "a"}, Asset: "USD"}] = struct{}{}
			},
		},
		{
			attrCode:  dal.SubAttrReference,
			canonical: domain.TransactionReferenceKey{LedgerName: "L", Reference: "ref"}.Bytes(),
			add: func(n *Needs) {
				n.References[domain.TransactionReferenceKey{LedgerName: "L", Reference: "ref"}] = struct{}{}
			},
		},
		{
			attrCode:  dal.SubAttrMetadata,
			canonical: domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "a"}, Key: "k"}.Bytes(),
			add: func(n *Needs) {
				n.Metadata[domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "a"}, Key: "k"}] = struct{}{}
			},
		},
		{
			attrCode:  dal.SubAttrTransaction,
			canonical: domain.TransactionKey{LedgerName: "L", ID: 1}.Bytes(),
			add:       func(n *Needs) { n.Transactions[domain.TransactionKey{LedgerName: "L", ID: 1}] = struct{}{} },
		},
		{
			attrCode:  dal.SubAttrSinkConfig,
			canonical: domain.SinkConfigKey{Name: "s"}.Bytes(),
			add:       func(n *Needs) { n.SinkConfigs[domain.SinkConfigKey{Name: "s"}] = struct{}{} },
		},
		{
			attrCode:  dal.SubAttrNumscriptVersion,
			canonical: domain.NumscriptVersionKey{LedgerName: "L", Name: "n"}.Bytes(),
			add: func(n *Needs) {
				n.NumscriptVersions[domain.NumscriptVersionKey{LedgerName: "L", Name: "n"}] = struct{}{}
			},
		},
		{
			attrCode:  dal.SubAttrNumscriptContent,
			canonical: domain.NumscriptEntryKey{LedgerName: "L", Name: "n", Version: "v"}.Bytes(),
			add: func(n *Needs) {
				n.NumscriptContents[domain.NumscriptEntryKey{LedgerName: "L", Name: "n", Version: "v"}] = struct{}{}
			},
		},
		{
			attrCode:  dal.SubAttrPreparedQuery,
			canonical: domain.PreparedQueryKey{LedgerName: "L", Name: "q"}.Bytes(),
			add: func(n *Needs) {
				n.PreparedQueries[domain.PreparedQueryKey{LedgerName: "L", Name: "q"}] = struct{}{}
			},
		},
		{
			attrCode:  dal.SubAttrLedgerMetadata,
			canonical: domain.LedgerMetadataKey{LedgerName: "L", Key: "k"}.Bytes(),
			add: func(n *Needs) {
				n.LedgerMetadata[domain.LedgerMetadataKey{LedgerName: "L", Key: "k"}] = struct{}{}
			},
		},
	}

	for _, c := range cases {
		u128, _ := attributes.MakeKey(c.canonical)
		plan := &raftcmdpb.AttributePlan{
			Id:       &raftcmdpb.AttributeID{Id: u128[:]},
			AttrCode: uint32(c.attrCode),
			Intent:   &raftcmdpb.AttributePlan_Declare{Declare: &raftcmdpb.Declare{}},
		}

		needs := NewNeeds()
		c.add(needs)

		require.Equal(t, []byte{0b1}, bitsForNeeds(needs, []*raftcmdpb.AttributePlan{plan}),
			"kind %d (%v) must flag bit 0 when its Needs entry is set", c.attrCode, c.canonical)
	}
}

// TestBitsForNeeds_EmptyInputs covers the nil/empty short-circuits.
func TestBitsForNeeds_EmptyInputs(t *testing.T) {
	t.Parallel()

	require.Nil(t, bitsForNeeds(nil, []*raftcmdpb.AttributePlan{{}}),
		"nil needs must yield nil bitset")

	require.Nil(t, bitsForNeeds(NewNeeds(), nil),
		"nil plans must yield nil bitset")
}

// TestBitsForNeeds_TracksPlanPosition pins the "recompute after guard
// rebuild" behaviour: Run calls bitsForNeeds before every marshal so a
// second call against a re-built attribute slice always reflects the
// latest plan position.
func TestBitsForNeeds_TracksPlanPosition(t *testing.T) {
	t.Parallel()

	const ledgerName = "alpha"
	u128, _ := attributes.MakeKey(domain.LedgerKey{Name: ledgerName}.Bytes())
	ledgerPlan := &raftcmdpb.AttributePlan{
		Id: &raftcmdpb.AttributeID{Id: u128[:]}, AttrCode: uint32(dal.SubAttrLedger),
		Intent: &raftcmdpb.AttributePlan_Declare{Declare: &raftcmdpb.Declare{}},
	}
	padding := &raftcmdpb.AttributePlan{
		Id: &raftcmdpb.AttributeID{Id: u128[:]}, AttrCode: uint32(dal.SubAttrBoundary),
		Intent: &raftcmdpb.AttributePlan_Declare{Declare: &raftcmdpb.Declare{}},
	}

	needs := NewNeeds()
	needs.Ledgers[domain.LedgerKey{Name: ledgerName}] = struct{}{}

	// First plan order: ledger at index 0 → bit 0.
	require.Equal(t, []byte{0b01}, bitsForNeeds(needs, []*raftcmdpb.AttributePlan{ledgerPlan, padding}))

	// Simulate a rebuild that put the ledger plan at index 1 → bit 1.
	require.Equal(t, []byte{0b10}, bitsForNeeds(needs, []*raftcmdpb.AttributePlan{padding, ledgerPlan}),
		"rebuild must produce bits tracking the new plan position")
}

// TestApplyBits_SharesPlanIndexAcrossOperations pins the
// buildPlanIndex hoist: applyBits must build the planLookupKey→position
// map once per call and feed it to every WriteOperation, yet still
// emit a coverage bitset that mirrors only the per-operation Needs.
//
// This is the hot-path optimization that drops applyBits' cost from
// O(N · P) runtime.mapassign (one map rebuild per operation) to O(P)
// for a batch of N operations sharing the same proposal plans slice.
// A regression that goes back to per-op map building still passes
// TestBitsForNeeds_* — only a batch-level test catches it.
func TestApplyBits_SharesPlanIndexAcrossOperations(t *testing.T) {
	t.Parallel()

	const (
		ledgerA = "alpha"
		ledgerB = "beta"
	)

	idA, _ := attributes.MakeKey(domain.LedgerKey{Name: ledgerA}.Bytes())
	idB, _ := attributes.MakeKey(domain.LedgerKey{Name: ledgerB}.Bytes())

	plans := []*raftcmdpb.AttributePlan{
		{
			Id: &raftcmdpb.AttributeID{Id: idA[:]}, AttrCode: uint32(dal.SubAttrLedger),
			Intent: &raftcmdpb.AttributePlan_Declare{Declare: &raftcmdpb.Declare{}},
		},
		{
			Id: &raftcmdpb.AttributeID{Id: idB[:]}, AttrCode: uint32(dal.SubAttrLedger),
			Intent: &raftcmdpb.AttributePlan_Declare{Declare: &raftcmdpb.Declare{}},
		},
	}

	needsA := NewNeeds()
	needsA.Ledgers[domain.LedgerKey{Name: ledgerA}] = struct{}{}

	needsB := NewNeeds()
	needsB.Ledgers[domain.LedgerKey{Name: ledgerB}] = struct{}{}

	needsAB := NewNeeds()
	needsAB.Ledgers[domain.LedgerKey{Name: ledgerA}] = struct{}{}
	needsAB.Ledgers[domain.LedgerKey{Name: ledgerB}] = struct{}{}

	var (
		gotA  []byte
		gotB  []byte
		gotAB []byte
		gotZ  []byte
		gotN  bool // SetCoverage(nil) for the no-needs op
	)

	build := &BuildResult{
		operations: []WriteOperation{
			{Needs: needsA, SetCoverage: func(b []byte) { gotA = b }},
			{Needs: needsB, SetCoverage: func(b []byte) { gotB = b }},
			{Needs: needsAB, SetCoverage: func(b []byte) { gotAB = b }},
			{Needs: NewNeeds(), SetCoverage: func(b []byte) { gotZ = b }},
			{Needs: nil, SetCoverage: func(b []byte) { gotN = true }},
			{Needs: needsA, SetCoverage: nil}, // skip — nil callback, must not panic
		},
	}

	build.applyBits(nil, plans)

	require.Equal(t, []byte{0b01}, gotA, "op A flags only bit 0 (ledgerA at index 0)")
	require.Equal(t, []byte{0b10}, gotB, "op B flags only bit 1 (ledgerB at index 1)")
	require.Equal(t, []byte{0b11}, gotAB, "op AB flags both bits")
	require.Equal(t, []byte{0b00}, gotZ, "op with empty Needs gets a zero bitset, not nil")
	require.True(t, gotN, "nil Needs op must still be invoked with nil per the original contract")
	// gotN's nil-input path returns nil from bitsForNeedsWithIndex; the
	// callback simply records that it ran.
}

// TestApplyBits_EmptyPlansPreservesNilContract pins the no-plan branch
// of applyBits: when the proposal carries zero AttributePlan entries
// (every WriteOperation has empty Needs, common for technical-only
// proposals), every SetCoverage callback must still fire with nil to
// keep the original bitsForNeeds(_, nil) → nil contract that handlers
// rely on (a zero-length bitset is semantically different from a
// missing one in the FSM's coverage check).
func TestApplyBits_EmptyPlansPreservesNilContract(t *testing.T) {
	t.Parallel()

	calls := make([]struct {
		called bool
		got    []byte
	}, 3)

	build := &BuildResult{
		operations: []WriteOperation{
			{Needs: NewNeeds(), SetCoverage: func(b []byte) { calls[0].called = true; calls[0].got = b }},
			{Needs: nil, SetCoverage: func(b []byte) { calls[1].called = true; calls[1].got = b }},
			{Needs: NewNeeds(), SetCoverage: nil}, // must be skipped silently
		},
	}

	build.applyBits(nil, nil)

	require.True(t, calls[0].called, "non-nil-callback op must be invoked even with empty plans")
	require.Nil(t, calls[0].got, "empty plans must yield nil bitset (not zero-length)")
	require.True(t, calls[1].called)
	require.Nil(t, calls[1].got)
}
