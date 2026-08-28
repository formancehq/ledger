package main

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// A returned volume set must match the base exactly: a ghost cell under ANY
// asset (not just the probed one — the stranded-row class), an omitted cell,
// and a divergent value are all mismatches.
func TestAccountVolumesMatch(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.modelState = c.modelState.Apply(bulkOf(
		oracletest.TxReq("world", "t-0:5", "USD/2", 10),
		oracletest.TxReq("world", "t-0:5", "COIN", 3),
	)).State
	ls := c.modelState.Ledger("L")

	exact := map[string]oracle.VolumePair{}
	for k, vp := range ls.Volumes().All() {
		if k.Address == "t-0:5" {
			exact[k.Asset] = vp
		}
	}
	require.Len(t, exact, 2)
	require.True(t, accountVolumesMatch(ls, "t-0:5", exact))

	ghost := maps.Clone(exact)
	var g oracle.VolumePair
	g.Input.SetUint64(7)
	g.Output.SetUint64(7)
	ghost["EUR/2"] = g
	require.False(t, accountVolumesMatch(ls, "t-0:5", ghost), "ghost cell under an unprobed asset must mismatch")

	omitted := maps.Clone(exact)
	delete(omitted, "USD/2")
	require.False(t, accountVolumesMatch(ls, "t-0:5", omitted), "omitted cell must mismatch")

	divergent := maps.Clone(exact)
	d := divergent["USD/2"]
	d.Input.AddUint64(&d.Input, 1)
	divergent["USD/2"] = d
	require.False(t, accountVolumesMatch(ls, "t-0:5", divergent))

	// An account the base doesn't hold: only the empty reading matches.
	require.True(t, accountVolumesMatch(ls, "t-9:9", nil))
	require.False(t, accountVolumesMatch(ls, "t-9:9", map[string]oracle.VolumePair{"USD/2": {}}))
}

// A read must pin past observed-but-undrained successes: tryDrain may fold
// them while the read is in flight, and a committedSeq-only pin would let the
// server serve a snapshot beneath the folded state — one no candidate base
// can represent.
func TestObservedFrontier_CoversPendingSuccesses(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.committedSeq = 10

	require.Equal(t, uint64(10), c.observedFrontier(), "no pending: frontier is the drained one")

	c.insertPending(&pendingObservation{minSeq: 11, obs: observation{
		resp: &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: 11}, {Sequence: 13}}},
	}})
	c.insertPending(&pendingObservation{minSeq: 14, obs: observation{
		resp: &servicepb.ApplyResponse{Logs: []*commonpb.Log{{Sequence: 14}}},
	}})

	require.Equal(t, uint64(14), c.observedFrontier(), "pending successes extend the frontier")
}
