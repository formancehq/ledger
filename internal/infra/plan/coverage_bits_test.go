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
// behind the gaming/lending/marketplace scenario panics: a Coverage that
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

	plans := []*raftcmdpb.AttributeCoverage{
		{
			Id: &raftcmdpb.AttributeID{Id: u128[:]}, AttrCode: uint32(dal.SubAttrLedger),
		},
		{
			Id: &raftcmdpb.AttributeID{Id: u128[:]}, AttrCode: uint32(dal.SubAttrBoundary),
		},
	}

	needs := NewCoverage()
	needs.Add(dal.SubAttrLedger, domain.LedgerKey{Name: ledgerName}.Bytes())
	needs.Add(dal.SubAttrBoundary, domain.LedgerKey{Name: ledgerName}.Bytes())

	require.Equal(t, []byte{0b11}, bitsForNeeds(needs, plans),
		"both Ledger (bit 0) and Boundary (bit 1) plans must be flagged even though they share a U128")
}

// TestBitsForNeeds_CoversEveryNeedsKind pins setIDInBitset's
// exhaustive dispatch over every Coverage map. A new field on Coverage that
// forgets a mark() arm here would silently never flag its bit in the
// coverage map, so an FSM read against that kind would surface as
// ErrCoverageMiss even when admission declared it.
func TestBitsForNeeds_CoversEveryNeedsKind(t *testing.T) {
	t.Parallel()

	type kindCase struct {
		attrCode  byte
		canonical []byte
		add       func(*Coverage)
	}

	cases := []kindCase{
		{
			attrCode:  dal.SubAttrLedger,
			canonical: domain.LedgerKey{Name: "L"}.Bytes(),
			add:       func(n *Coverage) { n.Add(dal.SubAttrLedger, domain.LedgerKey{Name: "L"}.Bytes()) },
		},
		{
			attrCode:  dal.SubAttrBoundary,
			canonical: domain.LedgerKey{Name: "L"}.Bytes(),
			add:       func(n *Coverage) { n.Add(dal.SubAttrBoundary, domain.LedgerKey{Name: "L"}.Bytes()) },
		},
		{
			attrCode:  dal.SubAttrVolume,
			canonical: domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "a"}, Asset: "USD"}.Bytes(),
			add: func(n *Coverage) {
				n.Add(dal.SubAttrVolume, domain.VolumeKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "a"}, Asset: "USD"}.Bytes())
			},
		},
		{
			attrCode:  dal.SubAttrReference,
			canonical: domain.TransactionReferenceKey{LedgerName: "L", Reference: "ref"}.Bytes(),
			add: func(n *Coverage) {
				n.Add(dal.SubAttrReference, domain.TransactionReferenceKey{LedgerName: "L", Reference: "ref"}.Bytes())
			},
		},
		{
			attrCode:  dal.SubAttrMetadata,
			canonical: domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "a"}, Key: "k"}.Bytes(),
			add: func(n *Coverage) {
				n.Add(dal.SubAttrMetadata, domain.MetadataKey{AccountKey: domain.AccountKey{LedgerName: "L", Account: "a"}, Key: "k"}.Bytes())
			},
		},
		{
			attrCode:  dal.SubAttrTransaction,
			canonical: domain.TransactionKey{LedgerName: "L", ID: 1}.Bytes(),
			add: func(n *Coverage) {
				n.Add(dal.SubAttrTransaction, domain.TransactionKey{LedgerName: "L", ID: 1}.Bytes())
			},
		},
		{
			attrCode:  dal.SubAttrSinkConfig,
			canonical: domain.SinkConfigKey{Name: "s"}.Bytes(),
			add:       func(n *Coverage) { n.Add(dal.SubAttrSinkConfig, domain.SinkConfigKey{Name: "s"}.Bytes()) },
		},
		{
			attrCode:  dal.SubAttrNumscriptVersion,
			canonical: domain.NumscriptVersionKey{LedgerName: "L", Name: "n"}.Bytes(),
			add: func(n *Coverage) {
				n.Add(dal.SubAttrNumscriptVersion, domain.NumscriptVersionKey{LedgerName: "L", Name: "n"}.Bytes())
			},
		},
		{
			attrCode:  dal.SubAttrNumscriptContent,
			canonical: domain.NumscriptEntryKey{LedgerName: "L", Name: "n", Version: "v"}.Bytes(),
			add: func(n *Coverage) {
				n.Add(dal.SubAttrNumscriptContent, domain.NumscriptEntryKey{LedgerName: "L", Name: "n", Version: "v"}.Bytes())
			},
		},
		{
			attrCode:  dal.SubAttrPreparedQuery,
			canonical: domain.PreparedQueryKey{LedgerName: "L", Name: "q"}.Bytes(),
			add: func(n *Coverage) {
				n.Add(dal.SubAttrPreparedQuery, domain.PreparedQueryKey{LedgerName: "L", Name: "q"}.Bytes())
			},
		},
		{
			attrCode:  dal.SubAttrLedgerMetadata,
			canonical: domain.LedgerMetadataKey{LedgerName: "L", Key: "k"}.Bytes(),
			add: func(n *Coverage) {
				n.Add(dal.SubAttrLedgerMetadata, domain.LedgerMetadataKey{LedgerName: "L", Key: "k"}.Bytes())
			},
		},
	}

	for _, c := range cases {
		u128, _ := attributes.MakeKey(c.canonical)
		plan := &raftcmdpb.AttributeCoverage{
			Id:       &raftcmdpb.AttributeID{Id: u128[:]},
			AttrCode: uint32(c.attrCode),
		}

		needs := NewCoverage()
		c.add(needs)

		require.Equal(t, []byte{0b1}, bitsForNeeds(needs, []*raftcmdpb.AttributeCoverage{plan}),
			"kind %d (%v) must flag bit 0 when its Coverage entry is set", c.attrCode, c.canonical)
	}
}

// TestBitsForNeeds_EmptyInputs covers the nil/empty short-circuits.
func TestBitsForNeeds_EmptyInputs(t *testing.T) {
	t.Parallel()

	require.Nil(t, bitsForNeeds(nil, []*raftcmdpb.AttributeCoverage{{}}),
		"nil needs must yield nil bitset")

	require.Nil(t, bitsForNeeds(NewCoverage(), nil),
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
	ledgerPlan := &raftcmdpb.AttributeCoverage{
		Id: &raftcmdpb.AttributeID{Id: u128[:]}, AttrCode: uint32(dal.SubAttrLedger),
	}
	padding := &raftcmdpb.AttributeCoverage{
		Id: &raftcmdpb.AttributeID{Id: u128[:]}, AttrCode: uint32(dal.SubAttrBoundary),
	}

	needs := NewCoverage()
	needs.Add(dal.SubAttrLedger, domain.LedgerKey{Name: ledgerName}.Bytes())

	// First plan order: ledger at index 0 → bit 0.
	require.Equal(t, []byte{0b01}, bitsForNeeds(needs, []*raftcmdpb.AttributeCoverage{ledgerPlan, padding}))

	// Simulate a rebuild that put the ledger plan at index 1 → bit 1.
	require.Equal(t, []byte{0b10}, bitsForNeeds(needs, []*raftcmdpb.AttributeCoverage{padding, ledgerPlan}),
		"rebuild must produce bits tracking the new plan position")
}

// TestApplyBits_SharesPlanIndexAcrossOperations pins the
// buildPlanIndex hoist: applyBits must build the planLookupKey→position
// map once per call and feed it to every WriteOperation, yet still
// emit a coverage bitset that mirrors only the per-operation Coverage.
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

	plans := []*raftcmdpb.AttributeCoverage{
		{
			Id: &raftcmdpb.AttributeID{Id: idA[:]}, AttrCode: uint32(dal.SubAttrLedger),
		},
		{
			Id: &raftcmdpb.AttributeID{Id: idB[:]}, AttrCode: uint32(dal.SubAttrLedger),
		},
	}

	needsA := NewCoverage()
	needsA.Add(dal.SubAttrLedger, domain.LedgerKey{Name: ledgerA}.Bytes())

	needsB := NewCoverage()
	needsB.Add(dal.SubAttrLedger, domain.LedgerKey{Name: ledgerB}.Bytes())

	needsAB := NewCoverage()
	needsAB.Add(dal.SubAttrLedger, domain.LedgerKey{Name: ledgerA}.Bytes())
	needsAB.Add(dal.SubAttrLedger, domain.LedgerKey{Name: ledgerB}.Bytes())

	var (
		gotA  []byte
		gotB  []byte
		gotAB []byte
		gotZ  []byte
		// Pre-seed gotN so the "target was written" assertion below can
		// distinguish "applyBits touched it" from "still initial value".
		gotN = []byte("sentinel")
	)

	build := &BuildResult{
		operations: []WriteOperation{
			{Coverage: needsA, Target: &gotA},
			{Coverage: needsB, Target: &gotB},
			{Coverage: needsAB, Target: &gotAB},
			{Coverage: NewCoverage(), Target: &gotZ},
			{Coverage: nil, Target: &gotN},
			{Coverage: needsA, Target: nil}, // skip — nil target, must not panic
		},
	}

	build.applyBits(nil, plans)

	require.Equal(t, []byte{0b01}, gotA, "op A flags only bit 0 (ledgerA at index 0)")
	require.Equal(t, []byte{0b10}, gotB, "op B flags only bit 1 (ledgerB at index 1)")
	require.Equal(t, []byte{0b11}, gotAB, "op AB flags both bits")
	require.Equal(t, []byte{0b00}, gotZ, "op with empty Coverage gets a zero bitset, not nil")
	require.Nil(t, gotN, "nil Coverage op still gets its target overwritten (nil bitset from bitsForNeedsWithIndex)")
}

// TestApplyBits_EmptyPlansPreservesNilContract pins the no-plan branch
// of applyBits: when the proposal carries zero AttributeCoverage entries
// (every WriteOperation has empty Coverage, common for technical-only
// proposals), every non-nil Target must still be overwritten with nil
// to keep the original bitsForNeeds(_, nil) → nil contract that handlers
// rely on (a zero-length bitset is semantically different from a
// missing one in the FSM's coverage check).
func TestApplyBits_EmptyPlansPreservesNilContract(t *testing.T) {
	t.Parallel()

	// Pre-seed targets so we can distinguish "applyBits wrote nil"
	// from "target left at initial value".
	got0 := []byte("initial-0")
	got1 := []byte("initial-1")
	got2 := []byte("initial-2")

	build := &BuildResult{
		operations: []WriteOperation{
			{Coverage: NewCoverage(), Target: &got0},
			{Coverage: nil, Target: &got1},
			{Coverage: NewCoverage(), Target: nil}, // must be skipped silently
		},
	}

	build.applyBits(nil, nil)

	require.Nil(t, got0, "non-nil-target op must be overwritten with nil bitset")
	require.Nil(t, got1, "nil-Coverage op still has its target written")
	require.Equal(t, []byte("initial-2"), got2, "nil-Target op must be untouched")
}
