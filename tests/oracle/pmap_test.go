package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// The fingerprint is the search's dedup identity: it must depend only on the
// entries a collection holds — not on the order, overwrites, or deletions that
// produced them.

func newTestMap() Map[string, int] {
	return NewMap[string, int](stringComparer{}, func(k string, v int) Digest {
		t := newTerm("test")
		t.str(k)
		t.u64(uint64(v))

		return t.sum()
	})
}

func TestMapFingerprint_PathIndependent(t *testing.T) {
	t.Parallel()

	a := newTestMap().Set("x", 1).Set("y", 2).Set("z", 3)
	b := newTestMap().Set("z", 3).Set("x", 7).Set("y", 2).Set("x", 1)
	require.Equal(t, a.Fingerprint(), b.Fingerprint(), "same entries, different history")

	c := a.Set("x", 9)
	require.NotEqual(t, a.Fingerprint(), c.Fingerprint(), "overwrite must change the fingerprint")

	d := a.Set("w", 4).Delete("w")
	require.Equal(t, a.Fingerprint(), d.Fingerprint(), "set+delete must round-trip")

	require.Equal(t, a.Fingerprint(), a.Delete("absent").Fingerprint(), "deleting an absent key is a no-op")

	var empty Digest
	require.Equal(t, empty, newTestMap().Fingerprint(), "empty map has the zero fingerprint")
}

func TestMapForksNeverAlias(t *testing.T) {
	t.Parallel()

	base := newTestMap().Set("x", 1)
	fork := base.Set("x", 2).Set("y", 3)

	v, ok := base.Get("x")
	require.True(t, ok)
	require.Equal(t, 1, v)
	require.False(t, base.Has("y"))

	v, _ = fork.Get("x")
	require.Equal(t, 2, v)
	require.Equal(t, 2, fork.Len())
	require.Equal(t, 1, base.Len())
}

func TestMapAll_SortedByKey(t *testing.T) {
	t.Parallel()

	m := newTestMap().Set("c", 3).Set("a", 1).Set("b", 2)

	var keys []string
	for k := range m.All() {
		keys = append(keys, k)
	}
	require.Equal(t, []string{"a", "b", "c"}, keys)
}

func TestListFingerprint_ReplaceRoundTrips(t *testing.T) {
	t.Parallel()

	newList := func() List[string] {
		return NewList[string](func(i int, v string) Digest {
			tb := newTerm("test-list")
			tb.u64(uint64(i))
			tb.str(v)

			return tb.sum()
		})
	}

	a := newList().Append("p").Append("q")
	b := newList().Append("p").Append("X").Set(1, "q")
	require.Equal(t, a.Fingerprint(), b.Fingerprint(), "replace must land on the same identity")

	c := a.Set(0, "r")
	require.NotEqual(t, a.Fingerprint(), c.Fingerprint())
	require.Equal(t, "p", a.Get(0), "fork must not see the replacement")
	require.Equal(t, a.Fingerprint(), c.Set(0, "p").Fingerprint(), "replacing back restores the identity")
}

// Commuting bulks folded in either order must land on the same fingerprint —
// the exact property candidateBases' dedup collapses serializations with.
func TestGlobalStateFingerprint_CommutingBulks(t *testing.T) {
	t.Parallel()

	base := NewGlobalState()
	b1 := Bulk{Requests: []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 5)}}
	b2 := Bulk{Requests: []*servicepb.Request{oracletest.TxReqL("L2", "world", "b:1", "EUR", 7)}}

	ab := base.Apply(b1).State.Apply(b2).State
	ba := base.Apply(b2).State.Apply(b1).State
	require.Equal(t, ab.Fingerprint(), ba.Fingerprint(), "commuting folds must dedup")
	require.NotEqual(t, ab.Fingerprint(), base.Apply(b1).State.Fingerprint())
}

// Apply materializes a ledger entry even when the operation stores nothing;
// such an entry must not change the state's identity (see Fingerprint).
// Materializing a ledger entry must not, by itself, change the global
// identity. Every committed order now appends a log, so a committed order can
// no longer leave an entry empty: the two reachable cases are a rejection
// (nothing materializes) and a commit (the log is state, and it counts).
func TestGlobalStateFingerprint_EmptyLedgerMaterialization(t *testing.T) {
	t.Parallel()

	base := NewGlobalState().Apply(Bulk{Requests: []*servicepb.Request{oracletest.TxReq("world", "a:1", "USD", 5)}}).State

	// A rejected order leaves the prior state untouched — the entry it would
	// have materialized is discarded with the fork, so identity is unchanged
	// and the ledger does not appear at all.
	rejected := base.Apply(Bulk{Requests: []*servicepb.Request{{
		Type: &servicepb.Request_RemoveAccountType{
			RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
				Ledger: "untouched",
				Name:   "never-declared",
			},
		},
	}}})
	require.False(t, rejected.OK)
	require.NotContains(t, rejected.State.Ledgers(), "untouched")
	require.Equal(t, base.Fingerprint(), rejected.State.Fingerprint(), "a rejected order must not change the identity")

	// Removing an undeclared field type changes no schema, but the server
	// emits its log unconditionally (processRemoveMetadataFieldType returns a
	// payload whether or not the key was declared), so the ledger gains a log
	// and with it an identity.
	noop := base.Apply(Bulk{Requests: []*servicepb.Request{{
		Type: &servicepb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
				Ledger:     "untouched",
				TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
				Key:        "k0",
			},
		},
	}}})
	require.True(t, noop.OK)
	require.False(t, noop.State.Ledger("untouched").IsEmpty(), "the committed log is state")
	require.NotEqual(t, base.Fingerprint(), noop.State.Fingerprint())
}
