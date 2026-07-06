package oracle

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// dec renders a uint256 volume value (Dec has a pointer receiver, so this
// takes an addressable copy for call sites on non-addressable map/return values).
func dec(v uint256.Int) string { return v.Dec() }

func bulkOf(reqs ...*servicepb.Request) Bulk { return Bulk{Requests: reqs} }

func TestGlobalState_Apply_ChartOps(t *testing.T) {
	t.Parallel()

	base := NewGlobalState()

	added := base.Apply(bulkOf(oracletest.AddTypeReq("T")))
	require.True(t, added.OK)
	require.Contains(t, added.State.Ledger("L").types, "T")
	// Immutability: deriving `added` never touched base.
	require.NotContains(t, base.Ledger("L").types, "T")

	dup := added.State.Apply(bulkOf(oracletest.AddTypeReq("T")))
	require.False(t, dup.OK)
	require.Equal(t, domain.ErrReasonAccountTypeAlreadyExists, dup.Reason)

	removed := added.State.Apply(bulkOf(oracletest.RemoveTypeReq("T")))
	require.True(t, removed.OK)
	require.NotContains(t, removed.State.Ledger("L").types, "T")

	gone := removed.State.Apply(bulkOf(oracletest.RemoveTypeReq("T")))
	require.False(t, gone.OK)
	require.Equal(t, domain.ErrReasonAccountTypeNotFound, gone.Reason)
}

func TestGlobalState_Apply_AtomicRejection(t *testing.T) {
	t.Parallel()

	// Second request fails (remove of a non-existent type) -> the first
	// request's add must roll back.
	res := NewGlobalState().Apply(bulkOf(oracletest.AddTypeReq("A"), oracletest.RemoveTypeReq("missing")))
	require.False(t, res.OK)
	require.Equal(t, domain.ErrReasonAccountTypeNotFound, res.Reason)
	require.Empty(t, res.State.Ledger("L").types)
}

func TestGlobalState_Apply_CrossLedgerAtomicRejection(t *testing.T) {
	t.Parallel()

	// A bulk spanning two ledgers: a fine transaction on A, then a doomed
	// remove on B. The whole bulk fails atomically, so A's transaction must
	// NOT commit — the case a per-ledger model could not represent.
	res := NewGlobalState().Apply(Bulk{Requests: []*servicepb.Request{
		oracletest.TxReqL("A", "world", "x:1", "USD", 5),
		oracletest.RemoveReqL("B", "missing"),
	}})
	require.False(t, res.OK)
	require.Equal(t, domain.ErrReasonAccountTypeNotFound, res.Reason)

	a := res.State.Ledger("A")
	require.NotContains(t, a.volumes, VolumeKey{"x:1", "USD"})
	require.NotContains(t, a.volumes, VolumeKey{"world", "USD"})
}

func TestGlobalState_Apply_CrossLedgerCommit(t *testing.T) {
	t.Parallel()

	// Both requests succeed on distinct ledgers; each ledger gets its own cell.
	res := NewGlobalState().Apply(Bulk{Requests: []*servicepb.Request{
		oracletest.TxReqL("A", "world", "x:1", "USD", 5),
		oracletest.TxReqL("B", "world", "y:1", "USD", 7),
	}})
	require.True(t, res.OK)

	a := res.State.Ledger("A")
	b := res.State.Ledger("B")
	require.Equal(t, "5", dec(a.vol(VolumeKey{"x:1", "USD"}).Input))
	require.Equal(t, "7", dec(b.vol(VolumeKey{"y:1", "USD"}).Input))
	require.NotContains(t, a.volumes, VolumeKey{"y:1", "USD"})
}

func TestGlobalState_Apply_TxEnforcement(t *testing.T) {
	t.Parallel()

	// Empty chart: enforcement is off, any address is allowed.
	empty := NewGlobalState().Apply(bulkOf(oracletest.TxReq("world", "x:1", "USD", 5)))
	require.True(t, empty.OK)

	// With a type declared, STRICT enforcement rejects an unmatched address...
	withType := NewGlobalState().Apply(bulkOf(oracletest.AddTypeReq("t")))
	require.True(t, withType.OK)

	bad := withType.State.Apply(bulkOf(oracletest.TxReq("world", "x:1", "USD", 5)))
	require.False(t, bad.OK)
	require.Equal(t, domain.ErrReasonAccountNotMatchingType, bad.Reason)

	// ...but a matching address (and world) is fine.
	good := withType.State.Apply(bulkOf(oracletest.TxReq("world", "t:1", "USD", 5)))
	require.True(t, good.OK)
}

func TestGlobalState_Apply_Volumes(t *testing.T) {
	t.Parallel()

	res := NewGlobalState().Apply(bulkOf(oracletest.TxReq("world", "a:1", "USD", 5)))
	require.True(t, res.OK)

	// Per-tx PCV: world out=5, a:1 in=5.
	pcv := res.Orders[0].PCV
	require.Equal(t, "5", dec(pcv[VolumeKey{"world", "USD"}].Output))
	require.Equal(t, "0", dec(pcv[VolumeKey{"world", "USD"}].Input))
	require.Equal(t, "5", dec(pcv[VolumeKey{"a:1", "USD"}].Input))

	// Persisted into the resulting state (no type -> no purge).
	ls := res.State.Ledger("L")
	require.Equal(t, "5", dec(ls.vol(VolumeKey{"a:1", "USD"}).Input))
}

func TestGlobalState_Apply_TransientNonZero(t *testing.T) {
	t.Parallel()

	s := NewGlobalState().Apply(bulkOf(oracletest.AddTypeReqP("t", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT))).State

	// A single inflow leaves t:1 non-zero at end of bulk -> rejected.
	bad := s.Apply(bulkOf(oracletest.TxReq("world", "t:1", "USD", 5)))
	require.False(t, bad.OK)
	require.Equal(t, domain.ErrReasonTransientAccountNonZero, bad.Reason)

	// Balanced within the bulk (in then out) -> commits, and t:1 is purged.
	good := s.Apply(bulkOf(oracletest.TxReq("world", "t:1", "USD", 5), oracletest.TxReq("t:1", "world", "USD", 5)))
	require.True(t, good.OK)
	require.NotContains(t, good.State.Ledger("L").volumes, VolumeKey{"t:1", "USD"})
}

func TestGlobalState_Apply_TransientGrandfather(t *testing.T) {
	t.Parallel()

	// Empty chart: world->g:1 leaves g:1 with a persisted, non-zero balance
	// (g:1 matches no type, so it isn't purged).
	s := NewGlobalState().Apply(bulkOf(oracletest.TxReq("world", "g:1", "USD", 5))).State
	sl := s.Ledger("L")
	require.Equal(t, "5", dec(sl.vol(VolumeKey{"g:1", "USD"}).Input))

	// A bulk now declares g as TRANSIENT and touches g:1 again, leaving it
	// non-zero. The pre-existing balance grandfathers it, so the bulk commits
	// rather than failing TRANSIENT_ACCOUNT_NON_ZERO.
	res := s.Apply(bulkOf(
		oracletest.AddTypeReqP("g", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT),
		oracletest.TxReq("world", "g:1", "USD", 3),
	))
	require.True(t, res.OK)
	rl := res.State.Ledger("L")
	require.Equal(t, "8", dec(rl.vol(VolumeKey{"g:1", "USD"}).Input))
}

func TestGlobalState_Apply_EphemeralPurge(t *testing.T) {
	t.Parallel()

	s := NewGlobalState().Apply(bulkOf(oracletest.AddTypeReqP("e", commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL))).State

	// Non-zero EPHEMERAL persists; zero-balance EPHEMERAL is purged.
	nonZero := s.Apply(bulkOf(oracletest.TxReq("world", "e:1", "USD", 5)))
	require.True(t, nonZero.OK)
	nl := nonZero.State.Ledger("L")
	require.Equal(t, "5", dec(nl.vol(VolumeKey{"e:1", "USD"}).Input))

	zeroed := s.Apply(bulkOf(oracletest.TxReq("world", "e:1", "USD", 5), oracletest.TxReq("e:1", "world", "USD", 5)))
	require.True(t, zeroed.OK)
	require.NotContains(t, zeroed.State.Ledger("L").volumes, VolumeKey{"e:1", "USD"})
}

// MetaValueString must render every MetadataValue wire kind with a distinct,
// type-tagged prefix, so the checker compares stored values exactly across all
// kinds (the server stores them verbatim).
func TestMetaValueString(t *testing.T) {
	t.Parallel()

	cases := map[string]*commonpb.MetadataValue{
		"s:hi":    commonpb.NewStringValue("hi"),
		"i:-42":   commonpb.NewIntValue(-42),
		"u:42":    commonpb.NewUintValue(42),
		"b:true":  commonpb.NewBoolValue(true),
		"n:orig":  commonpb.NewNullValue("orig"),
		"d:-1000": commonpb.NewDatetimeValue(-1000),
	}
	for want, v := range cases {
		require.Equal(t, want, MetaValueString(v))
	}
}

func TestApplyTransaction_BalanceFloor(t *testing.T) {
	t.Parallel()

	// Fund x:1 with 10 from world (world is overdraftable, so this always commits).
	funded := NewGlobalState().Apply(bulkOf(oracletest.TxReq("world", "x:1", "USD", 10)))
	require.True(t, funded.OK)

	// A non-forced debit within balance commits and moves the source's output.
	ok := funded.State.Apply(bulkOf(oracletest.TxReq("x:1", "y:1", "USD", 6)))
	require.True(t, ok.OK)
	okLedger := ok.State.Ledger("L")
	require.Equal(t, "6", dec(okLedger.vol(VolumeKey{"x:1", "USD"}).Output))

	// Exactly the balance is allowed (input >= output+amount, equality passes).
	require.True(t, funded.State.Apply(bulkOf(oracletest.TxReq("x:1", "y:1", "USD", 10))).OK)

	// One over the balance is rejected with INSUFFICIENT_FUNDS; nothing commits.
	over := funded.State.Apply(bulkOf(oracletest.TxReq("x:1", "y:1", "USD", 11)))
	require.False(t, over.OK)
	require.Equal(t, domain.ErrReasonInsufficientFunds, over.Reason)
	overLedger := over.State.Ledger("L")
	require.Equal(t, "0", dec(overLedger.vol(VolumeKey{"x:1", "USD"}).Output))

	// Force skips the floor: an over-balance forced debit commits.
	require.True(t, funded.State.Apply(bulkOf(oracletest.TxReqForce("x:1", "y:1", "USD", 1000, true))).OK)

	// world is never floored, regardless of the amount.
	require.True(t, NewGlobalState().Apply(bulkOf(oracletest.TxReq("world", "z:1", "USD", 1_000_000))).OK)
}

func TestApplyTransaction_BalanceFloor_RunningVolumes(t *testing.T) {
	t.Parallel()

	// Within one bulk a later transaction may spend what an earlier one funded:
	// the floor reads the running volumes, not the drained base.
	chain := NewGlobalState().Apply(bulkOf(
		oracletest.TxReq("world", "a:1", "USD", 10),
		oracletest.TxReq("a:1", "b:1", "USD", 10),
	))
	require.True(t, chain.OK)

	// The same debit without the funding leg is rejected.
	bare := NewGlobalState().Apply(bulkOf(oracletest.TxReq("a:1", "b:1", "USD", 10)))
	require.False(t, bare.OK)
	require.Equal(t, domain.ErrReasonInsufficientFunds, bare.Reason)
}

func TestApplyRevert_ForceSkipsFloor(t *testing.T) {
	t.Parallel()

	// tx1 funds x:1 with 10; x:1 then spends 6 (balance 4). Reverting tx1 debits
	// x:1 by 10 — more than it now holds — but reverts set force, so it commits.
	base := NewGlobalState().Apply(bulkOf(oracletest.TxReq("world", "x:1", "USD", 10)))
	require.True(t, base.OK)
	spent := base.State.Apply(bulkOf(oracletest.TxReq("x:1", "y:1", "USD", 6)))
	require.True(t, spent.OK)

	require.True(t, spent.State.Apply(bulkOf(oracletest.RevertReqL("L", 1, true))).OK)

	// Without force, the same revert would hit the floor.
	unforced := spent.State.Apply(bulkOf(oracletest.RevertReqL("L", 1, false)))
	require.False(t, unforced.OK)
	require.Equal(t, domain.ErrReasonInsufficientFunds, unforced.Reason)
}

func TestApplyTransaction_FloorBeforeChart(t *testing.T) {
	t.Parallel()

	// A ledger with one account type, so the chart is enforced. "acct:{id}"
	// matches acct:* but not x:1 / y:1.
	typed := NewGlobalState().Apply(bulkOf(oracletest.AddTypeReq("acct")))
	require.True(t, typed.OK)

	// A non-forced debit from an unfunded, chart-unmatched account: the server
	// checks the balance while producing postings, BEFORE validating account
	// types, so it reports INSUFFICIENT_FUNDS — not ACCOUNT_NOT_MATCHING_TYPE.
	underfunded := typed.State.Apply(bulkOf(oracletest.TxReq("x:1", "y:1", "USD", 5)))
	require.False(t, underfunded.OK)
	require.Equal(t, domain.ErrReasonInsufficientFunds, underfunded.Reason)

	// Funded from world (overdraftable, balance never fails) but the destination
	// is chart-unmatched: now the type check is the deciding rejection.
	unmatchedDest := typed.State.Apply(bulkOf(oracletest.TxReq("world", "y:1", "USD", 5)))
	require.False(t, unmatchedDest.OK)
	require.Equal(t, domain.ErrReasonAccountNotMatchingType, unmatchedDest.Reason)
}
