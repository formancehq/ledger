package numscriptgen_test

import (
	"math/big"
	"math/rand"
	"testing"

	gen "github.com/formancehq/ledger/v3/tests/numscriptgen"
	"github.com/formancehq/numscript"
	"github.com/stretchr/testify/require"
)

func TestPortionsListSumsToOne(t *testing.T) {
	// portionsList isn't exported; exercise it indirectly via many
	// generated programs and check every allotment clause set found sums
	// to 1 — or, with a `remaining` clause, to strictly less than 1 (the
	// oracle compile-rejects `remaining` on a block already at 100%, and a
	// sum above it is oracle/DIVERGENCES.md #6).
	for seed := range 200 {
		rng := rand.New(rand.NewSource(int64(seed)))
		p := gen.GenerateProgram(rng)
		for _, stmt := range p {
			checkSourcePortions(t, stmt.Source)
			checkDestinationPortions(t, stmt.Destination)
		}
	}
}

func checkSourcePortions(t *testing.T, s gen.Source) {
	t.Helper()
	switch s.Kind {
	case gen.SrcCapped:
		checkSourcePortions(t, *s.Inner)
	case gen.SrcInorder:
		for _, inner := range s.Sources {
			checkSourcePortions(t, inner)
		}
	case gen.SrcAllotment:
		total := new(big.Rat)
		for _, c := range s.Clauses {
			total.Add(total, c.Portion)
			checkSourcePortions(t, c.Source)
		}
		if s.AllotmentRemaining != nil {
			require.True(t, total.Cmp(big.NewRat(1, 1)) < 0, "source allotment portions with remaining must sum below 1, got %s", total)
			checkSourcePortions(t, *s.AllotmentRemaining)
		} else {
			require.True(t, total.Cmp(big.NewRat(1, 1)) == 0, "source allotment portions must sum to 1, got %s", total)
		}
	}
}

func checkDestinationPortions(t *testing.T, d gen.Destination) {
	t.Helper()
	switch d.Kind {
	case gen.DestInorder:
		for _, c := range d.InorderClauses {
			checkKeptOrDestPortions(t, c.KeptOrDest)
		}
		if d.Remaining != nil {
			checkKeptOrDestPortions(t, *d.Remaining)
		}
	case gen.DestAllotment:
		total := new(big.Rat)
		for _, c := range d.AllotClauses {
			total.Add(total, c.Portion)
			checkKeptOrDestPortions(t, c.KeptOrDest)
		}
		if d.AllotRemaining != nil {
			require.True(t, total.Cmp(big.NewRat(1, 1)) < 0, "destination allotment portions with remaining must sum below 1, got %s", total)
			checkKeptOrDestPortions(t, *d.AllotRemaining)
		} else {
			require.True(t, total.Cmp(big.NewRat(1, 1)) == 0, "destination allotment portions must sum to 1, got %s", total)
		}
	}
}

func checkKeptOrDestPortions(t *testing.T, k gen.KeptOrDest) {
	t.Helper()
	if k.Kind == gen.To {
		checkDestinationPortions(t, *k.Dest)
	}
}

func TestGenerateScriptParses(t *testing.T) {
	// Every generated script must be syntactically valid numscript. This
	// doesn't guarantee it also compiles against the oracle (that's the
	// difftest harness's job), but a parse failure here would mean the
	// generator/cleanup pass produced something structurally broken.
	for seed := range 500 {
		rng := rand.New(rand.NewSource(int64(seed)))
		_, _, _, script := gen.GenerateScript(rng)

		res := numscript.Parse(script).GetParsingErrors()
		require.Empty(t, res, "seed %d produced an unparseable script:\n%s", seed, script)
	}
}
