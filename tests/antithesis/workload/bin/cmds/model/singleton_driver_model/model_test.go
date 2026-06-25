package main

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/holiman/uint256"
)

func addTypeReqP(name string, p commonpb.AccountTypePersistence) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_AddAccountType{
			AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
				Ledger: "L",
				AccountType: &commonpb.AccountType{
					Name:        name,
					Pattern:     name + ":{id}",
					Persistence: p,
				},
			},
		},
	}
}

func addTypeReq(name string) *servicepb.Request {
	return addTypeReqP(name, commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL)
}

func removeReqL(ledger, name string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RemoveAccountType{
			RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{Ledger: ledger, Name: name},
		},
	}
}

func removeTypeReq(name string) *servicepb.Request {
	return removeReqL("L", name)
}

func txReqL(ledger, src, dest, asset string, amount int64) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewPosting(src, dest, asset, big.NewInt(amount)),
							},
						},
					},
				},
			},
		},
	}
}

func txReq(src, dest, asset string, amount int64) *servicepb.Request {
	return txReqL("L", src, dest, asset, amount)
}

func bulkOf(reqs ...*servicepb.Request) Bulk { return Bulk{Requests: reqs} }

// dec renders a uint256 volume value (Dec has a pointer receiver, so this
// takes an addressable copy for call sites on non-addressable map/return values).
func dec(v uint256.Int) string { return v.Dec() }

func TestGlobalState_Apply_ChartOps(t *testing.T) {
	t.Parallel()

	base := NewGlobalState()

	added := base.Apply(bulkOf(addTypeReq("T")))
	require.True(t, added.OK)
	require.Contains(t, added.State.ledger("L").types, "T")
	// Immutability: deriving `added` never touched base.
	require.NotContains(t, base.ledger("L").types, "T")

	dup := added.State.Apply(bulkOf(addTypeReq("T")))
	require.False(t, dup.OK)
	require.Equal(t, domain.ErrReasonAccountTypeAlreadyExists, dup.Reason)

	removed := added.State.Apply(bulkOf(removeTypeReq("T")))
	require.True(t, removed.OK)
	require.NotContains(t, removed.State.ledger("L").types, "T")

	gone := removed.State.Apply(bulkOf(removeTypeReq("T")))
	require.False(t, gone.OK)
	require.Equal(t, domain.ErrReasonAccountTypeNotFound, gone.Reason)
}

func TestGlobalState_Apply_AtomicRejection(t *testing.T) {
	t.Parallel()

	// Second request fails (remove of a non-existent type) -> the first
	// request's add must roll back.
	res := NewGlobalState().Apply(bulkOf(addTypeReq("A"), removeTypeReq("missing")))
	require.False(t, res.OK)
	require.Equal(t, domain.ErrReasonAccountTypeNotFound, res.Reason)
	require.Empty(t, res.State.ledger("L").types)
}

func TestGlobalState_Apply_CrossLedgerAtomicRejection(t *testing.T) {
	t.Parallel()

	// A bulk spanning two ledgers: a fine transaction on A, then a doomed
	// remove on B. The whole bulk fails atomically, so A's transaction must
	// NOT commit — the case a per-ledger model could not represent.
	res := NewGlobalState().Apply(Bulk{Requests: []*servicepb.Request{
		txReqL("A", "world", "x:1", "USD", 5),
		removeReqL("B", "missing"),
	}})
	require.False(t, res.OK)
	require.Equal(t, domain.ErrReasonAccountTypeNotFound, res.Reason)

	a := res.State.ledger("A")
	require.NotContains(t, a.volumes, VolumeKey{"x:1", "USD"})
	require.NotContains(t, a.volumes, VolumeKey{"world", "USD"})
}

func TestGlobalState_Apply_CrossLedgerCommit(t *testing.T) {
	t.Parallel()

	// Both requests succeed on distinct ledgers; each ledger gets its own cell.
	res := NewGlobalState().Apply(Bulk{Requests: []*servicepb.Request{
		txReqL("A", "world", "x:1", "USD", 5),
		txReqL("B", "world", "y:1", "USD", 7),
	}})
	require.True(t, res.OK)

	a := res.State.ledger("A")
	b := res.State.ledger("B")
	require.Equal(t, "5", dec(a.vol(VolumeKey{"x:1", "USD"}).Input))
	require.Equal(t, "7", dec(b.vol(VolumeKey{"y:1", "USD"}).Input))
	require.NotContains(t, a.volumes, VolumeKey{"y:1", "USD"})
}

func TestGlobalState_Apply_TxEnforcement(t *testing.T) {
	t.Parallel()

	// Empty chart: enforcement is off, any address is allowed.
	empty := NewGlobalState().Apply(bulkOf(txReq("world", "x:1", "USD", 5)))
	require.True(t, empty.OK)

	// With a type declared, STRICT enforcement rejects an unmatched address...
	withType := NewGlobalState().Apply(bulkOf(addTypeReq("t")))
	require.True(t, withType.OK)

	bad := withType.State.Apply(bulkOf(txReq("world", "x:1", "USD", 5)))
	require.False(t, bad.OK)
	require.Equal(t, domain.ErrReasonAccountNotMatchingType, bad.Reason)

	// ...but a matching address (and world) is fine.
	good := withType.State.Apply(bulkOf(txReq("world", "t:1", "USD", 5)))
	require.True(t, good.OK)
}

func TestGlobalState_Apply_Volumes(t *testing.T) {
	t.Parallel()

	res := NewGlobalState().Apply(bulkOf(txReq("world", "a:1", "USD", 5)))
	require.True(t, res.OK)

	// Per-tx PCV: world out=5, a:1 in=5.
	pcv := res.Orders[0].PCV
	require.Equal(t, "5", dec(pcv[VolumeKey{"world", "USD"}].Output))
	require.Equal(t, "0", dec(pcv[VolumeKey{"world", "USD"}].Input))
	require.Equal(t, "5", dec(pcv[VolumeKey{"a:1", "USD"}].Input))

	// Persisted into the resulting state (no type -> no purge).
	ls := res.State.ledger("L")
	require.Equal(t, "5", dec(ls.vol(VolumeKey{"a:1", "USD"}).Input))
}

func TestGlobalState_Apply_TransientNonZero(t *testing.T) {
	t.Parallel()

	s := NewGlobalState().Apply(bulkOf(addTypeReqP("t", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT))).State

	// A single inflow leaves t:1 non-zero at end of bulk -> rejected.
	bad := s.Apply(bulkOf(txReq("world", "t:1", "USD", 5)))
	require.False(t, bad.OK)
	require.Equal(t, domain.ErrReasonTransientAccountNonZero, bad.Reason)

	// Balanced within the bulk (in then out) -> commits, and t:1 is purged.
	good := s.Apply(bulkOf(txReq("world", "t:1", "USD", 5), txReq("t:1", "world", "USD", 5)))
	require.True(t, good.OK)
	require.NotContains(t, good.State.ledger("L").volumes, VolumeKey{"t:1", "USD"})
}

func TestGlobalState_Apply_TransientGrandfather(t *testing.T) {
	t.Parallel()

	// Empty chart: world->g:1 leaves g:1 with a persisted, non-zero balance
	// (g:1 matches no type, so it isn't purged).
	s := NewGlobalState().Apply(bulkOf(txReq("world", "g:1", "USD", 5))).State
	sl := s.ledger("L")
	require.Equal(t, "5", dec(sl.vol(VolumeKey{"g:1", "USD"}).Input))

	// A bulk now declares g as TRANSIENT and touches g:1 again, leaving it
	// non-zero. The pre-existing balance grandfathers it, so the bulk commits
	// rather than failing TRANSIENT_ACCOUNT_NON_ZERO.
	res := s.Apply(bulkOf(
		addTypeReqP("g", commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT),
		txReq("world", "g:1", "USD", 3),
	))
	require.True(t, res.OK)
	rl := res.State.ledger("L")
	require.Equal(t, "8", dec(rl.vol(VolumeKey{"g:1", "USD"}).Input))
}

func TestGlobalState_Apply_EphemeralPurge(t *testing.T) {
	t.Parallel()

	s := NewGlobalState().Apply(bulkOf(addTypeReqP("e", commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL))).State

	// Non-zero EPHEMERAL persists; zero-balance EPHEMERAL is purged.
	nonZero := s.Apply(bulkOf(txReq("world", "e:1", "USD", 5)))
	require.True(t, nonZero.OK)
	nl := nonZero.State.ledger("L")
	require.Equal(t, "5", dec(nl.vol(VolumeKey{"e:1", "USD"}).Input))

	zeroed := s.Apply(bulkOf(txReq("world", "e:1", "USD", 5), txReq("e:1", "world", "USD", 5)))
	require.True(t, zeroed.OK)
	require.NotContains(t, zeroed.State.ledger("L").volumes, VolumeKey{"e:1", "USD"})
}
