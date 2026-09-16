package main

import (
	"maps"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

// A returned volume set must match the base exactly: a ghost cell under ANY
// bucket (not just the probed one — the stranded-row class), an omitted cell,
// a divergent value, and a cell served under the wrong color are all
// mismatches.
func TestAccountVolumesMatch(t *testing.T) {
	t.Parallel()

	c := NewChecker([]string{"L"}, nil)
	c.modelState = c.modelState.Apply(bulkOf(
		oracletest.TxReq("world", "t-0:5", "USD/2", 10),
		oracletest.TxReq("world", "t-0:5", "COIN", 3),
		oracletest.TxReqColoredL("L", "world", "t-0:5", "USD/2", "GRANTS", 4),
	)).State
	ls := c.modelState.Ledger("L")

	exact := map[assetColor]oracle.VolumePair{}
	for k, vp := range ls.Volumes().All() {
		if k.Address == "t-0:5" {
			exact[assetColor{Asset: k.Asset, Color: k.Color}] = vp
		}
	}
	require.Len(t, exact, 3)
	require.True(t, accountVolumesMatch(ls, "t-0:5", exact))

	ghost := maps.Clone(exact)
	var g oracle.VolumePair
	g.Input.SetUint64(7)
	g.Output.SetUint64(7)
	ghost[assetColor{Asset: "EUR/2"}] = g
	require.False(t, accountVolumesMatch(ls, "t-0:5", ghost), "ghost cell under an unprobed asset must mismatch")

	omitted := maps.Clone(exact)
	delete(omitted, assetColor{Asset: "USD/2"})
	require.False(t, accountVolumesMatch(ls, "t-0:5", omitted), "omitted cell must mismatch")

	divergent := maps.Clone(exact)
	d := divergent[assetColor{Asset: "USD/2"}]
	d.Input.AddUint64(&d.Input, 1)
	divergent[assetColor{Asset: "USD/2"}] = d
	require.False(t, accountVolumesMatch(ls, "t-0:5", divergent))

	recoloured := maps.Clone(exact)
	grants := assetColor{Asset: "USD/2", Color: "GRANTS"}
	recoloured[assetColor{Asset: "USD/2", Color: "GOLD"}] = recoloured[grants]
	delete(recoloured, grants)
	require.False(t, accountVolumesMatch(ls, "t-0:5", recoloured), "the right amounts in the wrong bucket must mismatch")

	// An account the base doesn't hold: only the empty reading matches.
	require.True(t, accountVolumesMatch(ls, "t-9:9", nil))
	require.False(t, accountVolumesMatch(ls, "t-9:9", map[assetColor]oracle.VolumePair{{Asset: "USD/2"}: {}}))
}
