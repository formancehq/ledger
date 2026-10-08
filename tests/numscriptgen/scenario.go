package numscriptgen

import (
	"cmp"
	"fmt"
	"math/big"
	"math/rand"
	"slices"
	"strconv"
	"strings"
)

// A scenario block is a short run of statements sharing one (account, asset):
// setups that move the focus balance, then observers whose postings or
// pass/fail depend on it. Kinds are drawn uniformly, so any pair of a setup and
// an observer is as likely as any other.
const (
	setupSave          = "setup:save"
	setupSaveAll       = "setup:save-all"
	setupFund          = "setup:fund"
	setupDrainPlain    = "setup:drain-plain"
	setupDrawPlain     = "setup:draw-plain"
	setupDrawBounded   = "setup:draw-bounded"
	setupDrainBounded  = "setup:drain-bounded"
	setupDrawUnbounded = "setup:draw-unbounded"
	setupKeptSplit     = "setup:kept-split"

	observeDrainPlain   = "observe:drain-plain"
	observeDrainBounded = "observe:drain-bounded"
	observeDrawPlain    = "observe:draw-plain"
	observeDrawBounded  = "observe:draw-bounded"
	observeDrawFallback = "observe:draw-fallback"
	observeDrawCapped   = "observe:draw-capped"
)

var setupKinds = []string{
	setupSave, setupSaveAll, setupFund, setupDrainPlain, setupDrawPlain,
	setupDrawBounded, setupDrainBounded, setupDrawUnbounded, setupKeptSplit,
}

var observerKinds = []string{
	observeDrainPlain, observeDrainBounded, observeDrawPlain, observeDrawBounded,
	observeDrawFallback, observeDrawCapped,
}

// shadow is the generator's running estimate of the focus balance. It only
// steers amounts toward the values where an engine's arithmetic is decided; it
// is never asserted. Debits subtract and credits add with no floor or clamp,
// except where both engines are known to agree, so the estimate takes no side
// on how a clamp should behave.
type shadow struct {
	balance *big.Int
	// the last few amounts mentioned in the block
	quants []*big.Int
}

func (sh *shadow) note(q *big.Int) {
	sh.quants = append(sh.quants, q)
	if len(sh.quants) > 3 {
		sh.quants = sh.quants[1:]
	}
}

func (sh *shadow) add(q *big.Int) { sh.balance = new(big.Int).Add(sh.balance, q) }
func (sh *shadow) sub(q *big.Int) { sh.balance = new(big.Int).Sub(sh.balance, q) }

func (sh *shadow) clampTo(q *big.Int) {
	if sh.balance.Cmp(q) > 0 {
		sh.balance = new(big.Int).Set(q)
	}
}

// boundaryAmount is mostly an anchor plus an offset in {-1, 0, +1}, the
// anchors being zero, the estimated balance and the amounts already in the
// block (each also added to the balance). One draw in three is uniform, like
// monetary().
func boundaryAmount(rng *rand.Rand, sh *shadow) *big.Int {
	if rng.Intn(3) == 0 {
		return big.NewInt(int64(rng.Intn(1000)))
	}
	abs := new(big.Int).Abs(sh.balance)
	anchors := []*big.Int{big.NewInt(0), abs}
	for _, q := range sh.quants {
		anchors = append(anchors, q, new(big.Int).Add(abs, q))
	}
	out := new(big.Int).Add(anchors[rng.Intn(len(anchors))], big.NewInt(int64(rng.Intn(3)-1)))
	if out.Sign() < 0 {
		out.SetInt64(0)
	}
	return out
}

type scenarioGen struct {
	rng      *rand.Rand
	poolSize int
	focus    Focus
	focusIdx int
	sh       shadow
}

// sortedBalanceKeys returns the preset balance keys in a fixed order, so the
// seeded rng can index them.
func sortedBalanceKeys(balances map[BalanceKey]*big.Int) []BalanceKey {
	keys := make([]BalanceKey, 0, len(balances))
	for k := range balances {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b BalanceKey) int {
		if c := cmp.Compare(a.Account, b.Account); c != 0 {
			return c
		}
		return cmp.Compare(a.Asset, b.Asset)
	})
	return keys
}

// pickFocus draws the block's (account, asset) once. One draw in three comes
// from the preset balances, which is the only way to start from a negative
// balance.
func pickFocus(rng *rand.Rand, poolSize int, balances map[BalanceKey]*big.Int) (Focus, int) {
	if len(balances) > 0 && rng.Intn(3) == 0 {
		keys := sortedBalanceKeys(balances)
		k := keys[rng.Intn(len(keys))]
		idx, _ := strconv.Atoi(strings.TrimPrefix(k.Account, "acc"))
		return Focus(k), idx
	}
	idx := rng.Intn(poolSize)
	return Focus{Account: fmt.Sprintf("acc%d", idx), Asset: pickAsset(rng)}, idx
}

// genScenario builds one block and the Order index it is spliced at: 0 (right
// after the seeds, where the shadow estimate is exact) half of the time, else
// anywhere later.
func genScenario(rng *rand.Rand, poolSize int, seeds Program, balances map[BalanceKey]*big.Int, orderLen int) ([]ScenarioStmt, int, Focus) {
	focus, focusIdx := pickFocus(rng, poolSize, balances)
	pos := 0
	if orderLen > 0 && rng.Intn(2) != 0 {
		pos = 1 + rng.Intn(orderLen)
	}

	g := &scenarioGen{
		rng:      rng,
		poolSize: poolSize,
		focus:    focus,
		focusIdx: focusIdx,
		sh: shadow{
			balance: initialBalance(Script{Seeds: seeds, Balances: balances}, BalanceKey(focus)),
		},
	}

	setups := pick(rng, []weighted[int]{
		{3, func() int { return 1 }},
		{2, func() int { return 2 }},
		{1, func() int { return 3 }},
	})
	observers := 1
	if rng.Intn(4) == 0 {
		observers = 2
	}

	out := make([]ScenarioStmt, 0, setups+observers)
	for range setups {
		out = append(out, g.setup(setupKinds[rng.Intn(len(setupKinds))]))
	}
	for range observers {
		out = append(out, g.observe(observerKinds[rng.Intn(len(observerKinds))]))
	}
	return out, pos, focus
}

func (g *scenarioGen) amount() *big.Int {
	q := boundaryAmount(g.rng, &g.sh)
	g.sh.note(q)
	return q
}

func (g *scenarioGen) mon(amt *big.Int) Monetary {
	return Monetary{Asset: g.focus.Asset, AssetAsVar: asVar(g.rng), AsVar: asVar(g.rng), Amount: amt}
}

// otherAccount is a pool account other than the focus.
func (g *scenarioGen) otherAccount() string {
	return fmt.Sprintf("acc%d", (g.focusIdx+1+g.rng.Intn(g.poolSize-1))%g.poolSize)
}

func (g *scenarioGen) focusPlain() Source {
	return Source{Kind: SrcAccount, Account: g.focus.Account, AccountAsVar: asVar(g.rng)}
}

func (g *scenarioGen) focusBounded(od *big.Int) Source {
	m := g.mon(od)
	return Source{Kind: SrcAccountOverdraft, Account: g.focus.Account, AccountAsVar: asVar(g.rng), Overdraft: &m}
}

func (g *scenarioGen) focusUnbounded() Source {
	return Source{Kind: SrcAccountOverdraft, Account: g.focus.Account, AccountAsVar: asVar(g.rng)}
}

func (g *scenarioGen) otherUnbounded() Source {
	return Source{Kind: SrcAccountOverdraft, Account: g.otherAccount(), AccountAsVar: asVar(g.rng)}
}

func (g *scenarioGen) toOther() Destination {
	return Destination{Kind: DestAccount, Account: g.otherAccount(), AccountAsVar: asVar(g.rng)}
}

func (g *scenarioGen) toFocus() Destination {
	return Destination{Kind: DestAccount, Account: g.focus.Account, AccountAsVar: asVar(g.rng)}
}

func (g *scenarioGen) send(kind string, amt *big.Int, src Source, dest Destination) ScenarioStmt {
	st := cleanupStatement(Statement{Amount: g.mon(amt), Source: src, Destination: dest})
	return ScenarioStmt{Kind: kind, Send: &st}
}

func (g *scenarioGen) sendAll(kind string, src Source, dest Destination) ScenarioStmt {
	st := cleanupStatement(Statement{IsSendAll: true, Asset: g.focus.Asset, AssetAsVar: asVar(g.rng), Source: src, Destination: dest})
	return ScenarioStmt{Kind: kind, Send: &st}
}

func (g *scenarioGen) setup(kind string) ScenarioStmt {
	switch kind {
	case setupSave:
		m := g.mon(g.amount())
		g.sh.sub(m.Amount)
		return ScenarioStmt{Kind: kind, Extra: &ExtraStatement{
			Kind: ExtraSave, Monetary: &m, Account: g.focus.Account, AccountAsVar: asVar(g.rng),
		}}

	case setupSaveAll:
		// both engines zero a positive balance and leave a negative one
		g.sh.clampTo(big.NewInt(0))
		return ScenarioStmt{Kind: kind, Extra: &ExtraStatement{
			Kind: ExtraSaveAll, Asset: g.focus.Asset, AssetAsVar: asVar(g.rng),
			Account: g.focus.Account, AccountAsVar: asVar(g.rng),
		}}

	case setupFund:
		n := g.amount()
		g.sh.add(n)
		return g.send(kind, n, Source{Kind: SrcAccount, Account: "world"}, g.toFocus())

	case setupDrainPlain:
		// both engines take max(balance, 0)
		g.sh.clampTo(big.NewInt(0))
		return g.sendAll(kind, g.focusPlain(), g.toOther())

	case setupDrawPlain:
		n := g.amount()
		g.sh.sub(n)
		return g.send(kind, n, g.focusPlain(), g.toOther())

	case setupDrawBounded:
		od, n := g.amount(), g.amount()
		g.sh.sub(n)
		return g.send(kind, n, g.focusBounded(od), g.toOther())

	case setupDrainBounded:
		// both engines take max(balance + od, 0), leaving -od
		od := g.amount()
		g.sh.clampTo(new(big.Int).Neg(od))
		return g.sendAll(kind, g.focusBounded(od), g.toOther())

	case setupDrawUnbounded:
		n := g.amount()
		g.sh.sub(n)
		return g.send(kind, n, g.focusUnbounded(), g.toOther())

	case setupKeptSplit:
		n, k := g.amount(), g.amount()
		g.sh.sub(n)
		remaining := KeptOrDest{Kind: To, Dest: &Destination{Kind: DestAccount, Account: g.otherAccount(), AccountAsVar: asVar(g.rng)}}
		return g.send(kind, n,
			Source{Kind: SrcInorder, Sources: []Source{g.focusPlain(), g.otherUnbounded()}},
			Destination{
				Kind:           DestInorder,
				InorderClauses: []DestInorderClause{{Max: g.mon(k), KeptOrDest: KeptOrDest{Kind: Kept}}},
				Remaining:      &remaining,
			})

	default:
		panic("gen: unknown setup kind " + kind)
	}
}

func (g *scenarioGen) observe(kind string) ScenarioStmt {
	switch kind {
	case observeDrainPlain:
		g.sh.clampTo(big.NewInt(0))
		return g.sendAll(kind, g.focusPlain(), g.toOther())

	case observeDrainBounded:
		od := g.amount()
		g.sh.clampTo(new(big.Int).Neg(od))
		return g.sendAll(kind, g.focusBounded(od), g.toOther())

	case observeDrawPlain:
		n := g.amount()
		g.sh.sub(n)
		return g.send(kind, n, g.focusPlain(), g.toOther())

	case observeDrawBounded:
		od, n := g.amount(), g.amount()
		g.sh.sub(n)
		return g.send(kind, n, g.focusBounded(od), g.toOther())

	case observeDrawFallback:
		n := g.amount()
		g.sh.sub(n)
		first := g.focusPlain()
		if g.rng.Intn(2) == 0 {
			first = g.focusBounded(g.amount())
		}
		return g.send(kind, n,
			Source{Kind: SrcInorder, Sources: []Source{first, g.otherUnbounded()}},
			g.toOther())

	case observeDrawCapped:
		k, n := g.amount(), g.amount()
		g.sh.sub(n)
		capped := g.mon(k)
		inner := g.focusPlain()
		return g.send(kind, n,
			Source{Kind: SrcInorder, Sources: []Source{
				{Kind: SrcCapped, Cap: &capped, Inner: &inner},
				{Kind: SrcAccount, Account: "world"},
			}},
			g.toOther())

	default:
		panic("gen: unknown observer kind " + kind)
	}
}
