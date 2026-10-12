package numscriptgen_test

import (
	"math/big"
	"math/rand"
	"testing"

	gen "github.com/formancehq/ledger/v3/tests/numscriptgen"
	"github.com/formancehq/numscript"
	"github.com/stretchr/testify/require"
)

// Allotments mostly sum the way the engines accept — exactly 1, or under 1
// before a remaining clause — and sometimes miss, so the refusals are reached
// too.
func TestAllotmentSumsHitAndMiss(t *testing.T) {
	counts := map[string]int{}
	for seed := range 500 {
		rng := rand.New(rand.NewSource(int64(seed)))
		p := gen.GenerateProgram(rng)
		for _, stmt := range p {
			countSourceSums(stmt.Source, counts)
			countDestinationSums(stmt.Destination, counts)
		}
	}

	t.Logf("allotment sums: %v", counts)
	for _, k := range []string{"exact", "missed", "under-remaining", "over-remaining"} {
		require.Positive(t, counts[k], k)
	}
	require.Greater(t, counts["exact"], counts["missed"])
}

func classifySum(total *big.Rat, withRemaining bool, counts map[string]int) {
	one := big.NewRat(1, 1)
	switch {
	case withRemaining && total.Cmp(one) < 0:
		counts["under-remaining"]++
	case withRemaining:
		counts["over-remaining"]++
	case total.Cmp(one) == 0:
		counts["exact"]++
	default:
		counts["missed"]++
	}
}

func countSourceSums(s gen.Source, counts map[string]int) {
	switch s.Kind {
	case gen.SrcCapped:
		countSourceSums(*s.Inner, counts)
	case gen.SrcInorder, gen.SrcOneof:
		for _, inner := range s.Sources {
			countSourceSums(inner, counts)
		}
	case gen.SrcAllotment:
		total := new(big.Rat)
		for _, c := range s.Clauses {
			total.Add(total, c.Portion)
			countSourceSums(c.Source, counts)
		}
		classifySum(total, s.AllotmentRemaining != nil, counts)
	}
}

func countDestinationSums(d gen.Destination, counts map[string]int) {
	keptOrDest := func(k gen.KeptOrDest) {
		if k.Kind == gen.To {
			countDestinationSums(*k.Dest, counts)
		}
	}

	switch d.Kind {
	case gen.DestInorder, gen.DestOneof:
		for _, c := range d.InorderClauses {
			keptOrDest(c.KeptOrDest)
		}
		keptOrDest(*d.Remaining)
	case gen.DestAllotment:
		total := new(big.Rat)
		for _, c := range d.AllotClauses {
			total.Add(total, c.Portion)
			keptOrDest(c.KeptOrDest)
		}
		classifySum(total, d.AllotRemaining != nil, counts)
	}
}

func TestGenerateScriptParses(t *testing.T) {
	// Every generated script must be syntactically valid numscript: a parse
	// failure here means the generator produced something structurally broken.
	for seed := range 500 {
		rng := rand.New(rand.NewSource(int64(seed)))
		_, _, _, script := gen.GenerateScript(rng)

		res := numscript.Parse(script).GetParsingErrors()
		require.Empty(t, res, "seed %d produced an unparseable script:\n%s", seed, script)
	}
}
