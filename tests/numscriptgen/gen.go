package numscriptgen

import (
	"fmt"
	"math/big"
	"math/rand"
)

// maxPoolSize is the upper bound on the number of "acc0".."accN" accounts a
// single generated script can draw from ("world" is handled separately, as
// a literal, not part of this pool). The actual pool size is randomized per
// script (see pickPoolSize) — a small pool mechanically forces far more
// account reuse/collision across statements and within a single statement's
// source/destination trees than a always-16-accounts pool would.
const maxPoolSize = 15

// maxRecursionDepth is a hard cap on Source/Destination nesting. The
// probabilistic shaping below makes deep recursion unlikely but never
// impossible, so this is a backstop on top of it, not instead of it.
const maxRecursionDepth = 12

// assetPool is picked from per statement, not per script (see pickAsset).
var assetPool = []string{"COIN", "USD/2", "EUR/2"}

// ratio is the odds "num/denom" of an event.
type ratio struct {
	num, denom int
}

// smallerRat converges toward 1 as it is reapplied, so recursion and list
// length get likelier to stop the deeper generation goes.
func smallerRat(r ratio) ratio {
	return ratio{r.num + 1, r.denom + 1}
}

// weightedCoin returns true with probability r.num/r.denom.
func weightedCoin(rng *rand.Rand, r ratio) bool {
	return rng.Intn(r.denom) < r.num
}

// nonUniform picks an unbounded integer starting at n, recursing on
// (n+1, smallerRat(r)) with probability (1 - r.num/r.denom).
func nonUniform(rng *rand.Rand, r ratio, n int) int {
	if weightedCoin(rng, r) {
		return n
	}
	return nonUniform(rng, smallerRat(r), n+1)
}

// nonUniformListOf draws its length from nonUniform(1/10, 1).
func nonUniformListOf[T any](rng *rand.Rand, g func() T) []T {
	n := nonUniform(rng, ratio{1, 10}, 1)
	out := make([]T, n)
	for i := range out {
		out[i] = g()
	}
	return out
}

// portionsList returns random positive weights normalized into rationals.
// Without withRemaining they sum to exactly 1. With it they sum to strictly
// less than 1, for an allotment ending in a `remaining` clause: the oracle
// rejects `remaining` at compile time when the known portions already reach
// 100% (a tolerated skip that compares nothing), and above 100% both engines
// reject at runtime since the interpreter grew ledger's sum check
// (oracle/DIVERGENCES.md #6) but the literal form is still an oracle-side
// compile rejection, so every generated sum stays below both lines.
// One weight in ten is zeroed under withRemaining: a 0/N portion is legal on
// both engines and only representable when the slack keeps the total positive.
func portionsList(rng *rand.Rand, withRemaining bool) []*big.Rat {
	xs := nonUniformListOf(rng, func() int64 {
		return int64(rng.Intn(100) + 1)
	})

	total := int64(0)
	for _, x := range xs {
		total += x
	}

	if withRemaining {
		total += int64(rng.Intn(100)) + 1
		if rng.Intn(10) == 0 {
			xs[rng.Intn(len(xs))] = 0
		}
	}

	out := make([]*big.Rat, len(xs))
	for i, x := range xs {
		out[i] = big.NewRat(x, total)
	}
	return out
}

// pickPoolSize randomizes the number of pool accounts ("acc0"..) a script
// draws from, in [2, maxPoolSize] — sometimes as few as 2-3, forcing heavy
// reuse/collision; sometimes close to the old fixed 16, for broad coverage.
func pickPoolSize(rng *rand.Rand) int {
	return 2 + rng.Intn(maxPoolSize-1)
}

// pickAsset picks one asset name from assetPool. Called once per statement
// (not once per script), so different statements in the same script can use
// different assets, while a single statement's whole source/destination
// tree stays asset-consistent (matching real numscript semantics — a `max`
// clause's cap is implicitly the same asset as its enclosing send).
func pickAsset(rng *rand.Rand) string {
	return assetPool[rng.Intn(len(assetPool))]
}

// monetary generates a Monetary in the given asset. Mostly positive, at a low
// weight zero (exercising the zero-posting-trim path), and at a low weight
// beyond int64 (the engines are big.Int throughout, and the vm additionally
// encodes amounts — see instruction-encoding.md — so magnitude is a real axis).
//
// Never negative. A negative `max` clause amount is an open divergence: the
// oracle rejects it, the interpreter treats the clause as contributing nothing
// (oracle/DIVERGENCES.md #4). monetary() feeds every source cap and
// destination max, so allowing it would rediscover that on every run.
//
// A negative top-level send amount is different: both engines error, so
// statementAmount does emit those.
func monetary(rng *rand.Rand, asset string) Monetary {
	if rng.Intn(20) == 0 {
		return Monetary{Asset: asset, AssetAsVar: asVar(rng), AsVar: asVar(rng), Amount: big.NewInt(0)}
	}
	if rng.Intn(50) == 0 {
		return Monetary{Asset: asset, AssetAsVar: asVar(rng), AsVar: asVar(rng), Amount: hugeAmount(rng)}
	}
	return Monetary{Asset: asset, AssetAsVar: asVar(rng), AsVar: asVar(rng), Amount: big.NewInt(int64(rng.Intn(1000)))}
}

// hugeAmount draws an amount well past int64, so encodings and arithmetic see
// multi-word big.Ints.
func hugeAmount(rng *rand.Rand) *big.Int {
	x := new(big.Int).Lsh(big.NewInt(1), uint(65+rng.Intn(60)))
	return x.Add(x, big.NewInt(int64(rng.Intn(1000))))
}

// statementAmount generates a statement's top-level send amount. Unlike
// monetary() it includes negative values at a low weight: both engines error on
// those, with different text but the same outcome, which Compare tolerates.
//
// A negative amount has no literal form — convert.go renders it as
// `[ASSET 0] - [ASSET N]`.
func statementAmount(rng *rand.Rand, asset string) Monetary {
	roll := rng.Intn(100)
	switch {
	case roll < 5:
		return Monetary{Asset: asset, AssetAsVar: asVar(rng), AsVar: asVar(rng), Amount: big.NewInt(0)}
	case roll < 10:
		return Monetary{Asset: asset, AssetAsVar: asVar(rng), AsVar: asVar(rng), Amount: big.NewInt(-int64(rng.Intn(1000)) - 1)}
	default:
		return Monetary{Asset: asset, AssetAsVar: asVar(rng), AsVar: asVar(rng), Amount: big.NewInt(int64(rng.Intn(1000)))}
	}
}

func addMonetary(x *big.Int, m Monetary) Monetary {
	return Monetary{Asset: m.Asset, AssetAsVar: m.AssetAsVar, AsVar: m.AsVar, Amount: new(big.Int).Add(x, m.Amount)}
}

func account(rng *rand.Rand, poolSize int) string {
	return fmt.Sprintf("acc%d", rng.Intn(poolSize))
}

// accountAsVar decides the form of one account occurrence. "world" always stays
// inline: the oracle rejects any script in which a variable resolves to @world
// in source position, and Compare tolerates an oracle-side rejection, so a
// world-valued account var would silently skip the comparison instead of
// testing anything. The deliberate exploration of that asymmetry lives in
// AccountVarDecl, which is rationed; this is not.
func accountAsVar(rng *rand.Rand, account string) bool {
	if account == "world" {
		return false
	}
	return asVar(rng)
}

// asVar decides whether one value occurrence is written through a `vars {}`
// variable instead of inline. It is rolled per occurrence, so the same
// account or asset routinely appears both ways inside one script: that mix is
// the point, because two occurrences of one address in different forms become
// two distinct resources resolving to the same account (formancehq/ledger#2056).
// A uniform choice per value would never produce it.
func asVar(rng *rand.Rand) bool {
	return rng.Intn(3) == 0
}

func zeroFreqIf(weight int, cond bool) int {
	if cond {
		return 0
	}
	return weight
}

// weighted selection proportional to weight. Branches are thunks: only the
// chosen one runs, since evaluating the others would consume randomness.
type weighted[T any] struct {
	weight int
	build  func() T
}

func pick[T any](rng *rand.Rand, branches []weighted[T]) T {
	total := 0
	for _, b := range branches {
		total += b.weight
	}
	if total <= 0 {
		panic("gen: pick called with all-zero weights")
	}
	r := rng.Intn(total)
	for _, b := range branches {
		if r < b.weight {
			return b.build()
		}
		r -= b.weight
	}
	panic("gen: unreachable")
}

type sourceOptions struct {
	sentAmt                *big.Int
	isToplevel             bool
	isUnbounded            bool
	keepNestingProbability ratio
	poolSize               int
	asset                  string
	depth                  int
	// numscriptOnly, decided once per script, allows shapes the oracle cannot
	// parse (oneof, colors, wrong-asset caps, division portions). A script that
	// actually contains one skips the oracle legs — see scriptFlags.
	numscriptOnly bool
}

func defaultSourceOptions(sentAmt *big.Int, unbounded bool, poolSize int, asset string, numscriptOnly bool) sourceOptions {
	return sourceOptions{
		sentAmt:                sentAmt,
		isToplevel:             true,
		isUnbounded:            unbounded,
		keepNestingProbability: ratio{1, 15},
		poolSize:               poolSize,
		asset:                  asset,
		depth:                  0,
		numscriptOnly:          numscriptOnly,
	}
}

// capAsset picks the asset for a cap/max/overdraft-bound position: the
// statement's own asset, except at a low rate in numscript-only scripts, where
// a deliberately wrong one exercises each engine's same-asset assertion —
// including in clauses the interpreter never evaluates (it breaks out of a
// destination inorder once nothing remains, and both engines must agree on
// exactly when such a clause's failure is reachable).
func capAsset(rng *rand.Rand, asset string, numscriptOnly bool) string {
	if !numscriptOnly || rng.Intn(10) != 0 {
		return asset
	}
	for _, other := range assetPool {
		if other != asset {
			return other
		}
	}
	return asset
}

// srcColor decides an optional color clause on an account source. Only in
// numscript-only scripts (the oracle has no color syntax), at a low rate.
func srcColor(rng *rand.Rand, numscriptOnly bool) (string, bool) {
	if !numscriptOnly || rng.Intn(8) != 0 {
		return "", false
	}
	colors := []string{"RED", "BLU"}
	return colors[rng.Intn(len(colors))], asVar(rng)
}

// divPortion maybe replaces one allotment clause's portion with a division
// expression `$n/den`, whose value is unconstrained: negative (rejected by both
// engines), zero, ordinary, or above one. Only next to a `remaining` clause —
// without one the sum must be exactly 1 and nearly every draw would be a
// same-on-both-engines rejection that compares nothing.
func divPortion(rng *rand.Rand, numscriptOnly, withRemaining bool) *PortionDiv {
	if !numscriptOnly || !withRemaining || rng.Intn(6) != 0 {
		return nil
	}
	den := int64(rng.Intn(11) + 2)
	num := int64(rng.Intn(int(den)+5)) - 3
	return &PortionDiv{Num: big.NewInt(num), Den: big.NewInt(den)}
}

func genSource(rng *rand.Rand, opts sourceOptions) Source {
	stopRecursion := weightedCoin(rng, opts.keepNestingProbability)
	forceLeaf := opts.depth >= maxRecursionDepth

	nestedOpts := opts
	nestedOpts.isToplevel = false
	nestedOpts.keepNestingProbability = smallerRat(opts.keepNestingProbability)
	nestedOpts.depth = opts.depth + 1

	return pick(rng, []weighted[Source]{
		{
			zeroFreqIf(5, opts.isUnbounded),
			func() Source {
				color, colorAsVar := srcColor(rng, opts.numscriptOnly)
				return Source{Kind: SrcAccount, Account: "world", Color: color, ColorAsVar: colorAsVar}
			},
		},
		{
			15,
			func() Source {
				color, colorAsVar := srcColor(rng, opts.numscriptOnly)
				return Source{Kind: SrcAccount, Account: account(rng, opts.poolSize), AccountAsVar: asVar(rng), Color: color, ColorAsVar: colorAsVar}
			},
		},
		{
			5,
			func() Source {
				m := monetary(rng, capAsset(rng, opts.asset, opts.numscriptOnly))
				color, colorAsVar := srcColor(rng, opts.numscriptOnly)
				return Source{Kind: SrcAccountOverdraft, Account: account(rng, opts.poolSize), AccountAsVar: asVar(rng), Overdraft: &m, Color: color, ColorAsVar: colorAsVar}
			},
		},
		{
			zeroFreqIf(5, opts.isUnbounded),
			func() Source {
				color, colorAsVar := srcColor(rng, opts.numscriptOnly)
				return Source{Kind: SrcAccountOverdraft, Account: account(rng, opts.poolSize), AccountAsVar: asVar(rng), Overdraft: nil, Color: color, ColorAsVar: colorAsVar}
			},
		},
		{
			zeroFreqIf(5, forceLeaf),
			func() Source {
				// opts.sentAmt (the statement's own top-level amount) can be
				// negative — see statementAmount — but monetary()'s result
				// never is; clamp the base to >=0 so their sum (the cap)
				// can't land negative either. A negative `max` cap is a
				// real, confirmed divergence (see monetary's doc comment),
				// so this cap must stay non-negative.
				base := opts.sentAmt
				if base.Sign() < 0 {
					base = new(big.Int)
				}
				cap := addMonetary(base, monetary(rng, capAsset(rng, opts.asset, opts.numscriptOnly)))
				innerOpts := nestedOpts
				innerOpts.isUnbounded = false
				inner := genSource(rng, innerOpts)
				return Source{Kind: SrcCapped, Cap: &cap, Inner: &inner}
			},
		},
		{
			zeroFreqIf(10, stopRecursion || forceLeaf),
			func() Source {
				list := nonUniformListOf(rng, func() Source { return genSource(rng, nestedOpts) })
				return Source{Kind: SrcInorder, Sources: list}
			},
		},
		{
			zeroFreqIf(8, stopRecursion || forceLeaf || !opts.numscriptOnly),
			func() Source {
				list := nonUniformListOf(rng, func() Source { return genSource(rng, nestedOpts) })
				return Source{Kind: SrcOneof, Sources: list}
			},
		},
		{
			zeroFreqIf(15, stopRecursion || !opts.isToplevel || opts.isUnbounded || forceLeaf),
			func() Source {
				innerOpts := nestedOpts
				innerOpts.isUnbounded = false
				withRemaining := rng.Intn(2) == 0
				portions := portionsList(rng, withRemaining)
				clauses := make([]SourceAllotmentClause, len(portions))
				for i, p := range portions {
					clauses[i] = SourceAllotmentClause{
						Portion:      p,
						PortionAsVar: withRemaining && asVar(rng),
						Div:          divPortion(rng, opts.numscriptOnly, withRemaining),
						Source:       genSource(rng, innerOpts),
					}
				}
				src := Source{Kind: SrcAllotment, Clauses: clauses}
				if withRemaining {
					remaining := genSource(rng, innerOpts)
					src.AllotmentRemaining = &remaining
				}
				return src
			},
		},
	})
}

type destinationOptions struct {
	keepNestingProbability ratio
	poolSize               int
	asset                  string
	depth                  int
	// See sourceOptions.numscriptOnly.
	numscriptOnly bool
}

func defaultDestinationOptions(poolSize int, asset string, numscriptOnly bool) destinationOptions {
	return destinationOptions{keepNestingProbability: ratio{1, 15}, poolSize: poolSize, asset: asset, depth: 0, numscriptOnly: numscriptOnly}
}

func genDestination(rng *rand.Rand, opts destinationOptions) Destination {
	stopRecursion := weightedCoin(rng, opts.keepNestingProbability)
	forceLeaf := opts.depth >= maxRecursionDepth
	nestedOpts := destinationOptions{
		keepNestingProbability: smallerRat(opts.keepNestingProbability),
		poolSize:               opts.poolSize,
		asset:                  opts.asset,
		depth:                  opts.depth + 1,
		numscriptOnly:          opts.numscriptOnly,
	}

	inorderClauses := func() []DestInorderClause {
		return nonUniformListOf(rng, func() DestInorderClause {
			return DestInorderClause{Max: monetary(rng, capAsset(rng, opts.asset, opts.numscriptOnly)), KeptOrDest: genKeptOrDest(rng, nestedOpts)}
		})
	}

	return pick(rng, []weighted[Destination]{
		{
			30,
			func() Destination {
				return Destination{Kind: DestAccount, Account: account(rng, opts.poolSize), AccountAsVar: asVar(rng)}
			},
		},
		{
			zeroFreqIf(10, stopRecursion || forceLeaf),
			func() Destination {
				clauses := inorderClauses()
				remaining := genKeptOrDest(rng, nestedOpts)
				return Destination{Kind: DestInorder, InorderClauses: clauses, Remaining: &remaining}
			},
		},
		{
			zeroFreqIf(8, stopRecursion || forceLeaf || !opts.numscriptOnly),
			func() Destination {
				clauses := inorderClauses()
				remaining := genKeptOrDest(rng, nestedOpts)
				return Destination{Kind: DestOneof, InorderClauses: clauses, Remaining: &remaining}
			},
		},
		{
			zeroFreqIf(10, stopRecursion || forceLeaf),
			func() Destination {
				withRemaining := rng.Intn(2) == 0
				portions := portionsList(rng, withRemaining)
				clauses := make([]DestAllotmentClause, len(portions))
				for i, p := range portions {
					clauses[i] = DestAllotmentClause{
						Portion:      p,
						PortionAsVar: withRemaining && asVar(rng),
						Div:          divPortion(rng, opts.numscriptOnly, withRemaining),
						KeptOrDest:   genKeptOrDest(rng, nestedOpts),
					}
				}
				dest := Destination{Kind: DestAllotment, AllotClauses: clauses}
				if withRemaining {
					remaining := genKeptOrDest(rng, nestedOpts)
					dest.AllotRemaining = &remaining
				}
				return dest
			},
		},
	})
}

func genKeptOrDest(rng *rand.Rand, opts destinationOptions) KeptOrDest {
	return pick(rng, []weighted[KeptOrDest]{
		{1, func() KeptOrDest { return KeptOrDest{Kind: Kept} }},
		{3, func() KeptOrDest {
			d := genDestination(rng, opts)
			return KeptOrDest{Kind: To, Dest: &d}
		}},
	})
}

func genStatement(rng *rand.Rand, poolSize int, numscriptOnly bool) Statement {
	pickUnbounded := pick(rng, []weighted[bool]{
		{1, func() bool { return true }},
		{3, func() bool { return false }},
	})

	asset := pickAsset(rng)
	sent := statementAmount(rng, asset)
	src := genSource(rng, defaultSourceOptions(sent.Amount, pickUnbounded, poolSize, asset, numscriptOnly))
	dest := genDestination(rng, defaultDestinationOptions(poolSize, asset, numscriptOnly))

	if pickUnbounded {
		return Statement{IsSendAll: true, Asset: sent.Asset, Source: src, Destination: dest}
	}
	return Statement{IsSendAll: false, Amount: sent, Source: src, Destination: dest}
}

func genProgram(rng *rand.Rand, poolSize int, numscriptOnly bool) Program {
	return Program(nonUniformListOf(rng, func() Statement { return genStatement(rng, poolSize, numscriptOnly) }))
}

// GenerateProgram generates a random program (with its own randomized
// account-pool size) and applies the cleanup pass (cleanup.go) that removes
// constructs the legacy machine would reject.
func GenerateProgram(rng *rand.Rand) Program {
	return cleanupProgram(genProgram(rng, pickPoolSize(rng), false))
}
