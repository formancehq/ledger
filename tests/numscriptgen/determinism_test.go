package numscriptgen_test

import (
	"fmt"
	"maps"
	"math/rand"
	"testing"

	gen "github.com/formancehq/ledger/v3/tests/numscriptgen"
)

// The fuzzer saves a failing input as raw seed bytes, and Go's shrinker
// re-runs those bytes expecting the same program back. So generation must be
// a pure function of the seed. The subtle way to break that is to build a
// slice by ranging a map and then index it with the seeded rng: Go randomizes
// map iteration order, so the same seed picks a different element per run and
// a saved divergence can fail to reproduce.
//
// Several seeds are needed because the hazard only shows when a map holds two
// or more entries; a single-entry map has no order to vary.
func TestGenerationIsSeedDeterministic(t *testing.T) {
	for s := range 400 {
		seed := []byte(fmt.Sprintf("seed-%d", s))
		wantVars, wantBalances, wantMeta, want := gen.GenerateScript(gen.RandFromBytes(seed))

		for range 20 {
			gotVars, gotBalances, gotMeta, got := gen.GenerateScript(gen.RandFromBytes(seed))
			if got != want {
				t.Fatalf("seed %d generated two different programs:\n--- run A ---\n%s\n--- run B ---\n%s", s, want, got)
			}
			if len(gotVars) != len(wantVars) || len(gotBalances) != len(wantBalances) || len(gotMeta) != len(wantMeta) {
				t.Fatalf("seed %d generated the same program with different inputs", s)
			}
		}
	}
}

func TestRerollVarsIsSeedDeterministic(t *testing.T) {
	t.Parallel()

	vars := map[string]string{
		"account_0":  "a",
		"account_1":  "b",
		"asset_0":    "USD/2",
		"number_0":   "1",
		"monetary_0": "USD/2 10",
		"portion_0":  "1/2",
		"string_0":   "s",
	}
	accounts := []string{"x", "y", "z"}

	for s := range 50 {
		want := gen.RerollVars(rand.New(rand.NewSource(int64(s))), vars, accounts)
		for range 20 {
			got := gen.RerollVars(rand.New(rand.NewSource(int64(s))), vars, accounts)
			if !maps.Equal(got, want) {
				t.Fatalf("seed %d rerolled differently: %v vs %v", s, want, got)
			}
		}
	}
}
