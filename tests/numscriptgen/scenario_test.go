package numscriptgen

import (
	"math/rand"
	"testing"
)

// The sweep's seed space: rand.NewSource(seed) for seed in [0, n).
func scenarioScripts(n int) []Script {
	out := make([]Script, 0, n)
	for seed := range n {
		s := generateScriptAST(rand.New(rand.NewSource(int64(seed))))
		if len(s.Scenario) > 0 {
			out = append(out, s)
		}
	}
	return out
}

func TestBodyStatementsSpliceTheScenarioContiguously(t *testing.T) {
	for _, s := range scenarioScripts(3000) {
		body := bodyStatements(s)
		if len(body) != len(s.Order)+len(s.Scenario) {
			t.Fatalf("body has %d statements, want %d", len(body), len(s.Order)+len(s.Scenario))
		}
		start := -1
		for i, st := range body {
			if st.send == s.Scenario[0].Send && st.extra == s.Scenario[0].Extra {
				start = i
				break
			}
		}
		if start != s.ScenarioPos {
			t.Fatalf("scenario starts at %d, ScenarioPos is %d", start, s.ScenarioPos)
		}
		for j, sc := range s.Scenario {
			st := body[start+j]
			if st.send != sc.Send || st.extra != sc.Extra {
				t.Fatalf("scenario statement %d is not at body index %d", j, start+j)
			}
		}
	}
}

// Every kind must be reached often enough that a shape needing one setup and
// one observer is a matter of tens of scripts, not luck.
func TestScenarioKindsAreAllReached(t *testing.T) {
	seen := map[string]int{}
	byStrategy := map[Strategy]int{}
	for seed := range 3000 {
		s := generateScriptAST(rand.New(rand.NewSource(int64(seed))))
		byStrategy[s.Strategy]++
		for _, st := range s.Scenario {
			seen[st.Kind]++
		}
	}
	t.Logf("strategies: uniform=%d mixed=%d scenario-only=%d",
		byStrategy[StrategyUniform], byStrategy[StrategyScenarioMixed], byStrategy[StrategyScenarioOnly])
	for _, kind := range append(append([]string{}, setupKinds...), observerKinds...) {
		t.Logf("%-24s %d", kind, seen[kind])
		if seen[kind] < 20 {
			t.Errorf("%s reached only %d times", kind, seen[kind])
		}
	}
}

// The shape DIVERGENCES.md #3 needs: a save, then a bounded-overdraft draw on
// the same (account, asset). The uniform path reached it in 60 of 3000 seeds
// with the save overdrawing in 4; the block must make it common.
func TestScenariosReachSaveThenOverdraft(t *testing.T) {
	inOrder, overdraws := 0, 0
	for seed := range 3000 {
		sh := computeShape(generateScriptAST(rand.New(rand.NewSource(int64(seed)))))
		if sh.SaveOverdraftSameResourceInOrder {
			inOrder++
		}
		if sh.SaveOverdrawsInitial {
			overdraws++
		}
	}
	t.Logf("save then bounded overdraft on one (account, asset): %d/3000, save overdraws: %d/3000", inOrder, overdraws)
	if inOrder < 50 {
		t.Errorf("reached only %d times", inOrder)
	}
	if overdraws < 20 {
		t.Errorf("save overdraws only %d times", overdraws)
	}
}
