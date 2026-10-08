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

func hasUnboundedLeaf(src Source) bool {
	switch src.Kind {
	case SrcAccount, SrcAccountOverdraft:
		return isSrcUnbounded(src)
	case SrcCapped:
		return false
	case SrcInorder:
		for _, inner := range src.Sources {
			if hasUnboundedLeaf(inner) {
				return true
			}
		}
	case SrcAllotment:
		for _, c := range src.Clauses {
			if hasUnboundedLeaf(c.Source) {
				return true
			}
		}
	}
	return false
}

func checkSource(t *testing.T, src Source) {
	t.Helper()
	switch src.Kind {
	case SrcAccountOverdraft:
		if src.Account == "world" {
			t.Errorf("@world with an overdraft clause")
		}
	case SrcCapped:
		if src.Cap.Amount.Sign() < 0 {
			t.Errorf("negative cap %s", src.Cap.Amount)
		}
		checkSource(t, *src.Inner)
	case SrcInorder:
		for i, inner := range src.Sources {
			if hasUnboundedLeaf(inner) && i != len(src.Sources)-1 {
				t.Errorf("unbounded subsource at position %d of %d", i, len(src.Sources))
			}
			checkSource(t, inner)
		}
	case SrcAllotment:
		for _, c := range src.Clauses {
			checkSource(t, c.Source)
		}
	}
	if src.Kind == SrcAccountOverdraft && src.Overdraft != nil && src.Overdraft.Amount.Sign() < 0 {
		t.Errorf("negative overdraft %s", src.Overdraft.Amount)
	}
}

// A scenario statement the oracle rejects at compile time is silently skipped
// by Compare, so these shapes are checked here.
func TestScenarioStatementsStayWithinTheOracleGrammar(t *testing.T) {
	for _, s := range scenarioScripts(3000) {
		if s.Focus.Account == "world" {
			t.Fatal("focus on @world")
		}
		for _, st := range s.Scenario {
			if (st.Send == nil) == (st.Extra == nil) {
				t.Fatalf("%s: exactly one of Send and Extra must be set", st.Kind)
			}
			if st.Extra != nil {
				if st.Extra.Kind == ExtraSave && st.Extra.Monetary.Amount.Sign() < 0 {
					t.Errorf("%s: negative save", st.Kind)
				}
				continue
			}
			send := st.Send
			if !send.IsSendAll && send.Amount.Amount.Sign() < 0 {
				t.Errorf("%s: negative send amount", st.Kind)
			}
			if send.IsSendAll && hasUnboundedLeaf(send.Source) {
				t.Errorf("%s: send-all from an unbounded source", st.Kind)
			}
			accounts := sourceAccounts(send.Source, nil)
			seen := map[string]bool{}
			for _, a := range accounts {
				if seen[a] && a != "world" {
					t.Errorf("%s: account %s twice in one source", st.Kind, a)
				}
				seen[a] = true
			}
			checkSource(t, send.Source)
		}
	}
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
