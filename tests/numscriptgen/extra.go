package numscriptgen

import (
	"cmp"
	"fmt"
	"math/big"
	"math/rand"
	"slices"
)

// seedAmount is for `world -> accN` funding statements, always positive.
// Zero and negative amounts are explored by monetary() instead.
func seedAmount(rng *rand.Rand) *big.Int {
	return big.NewInt(int64(rng.Intn(1000)) + 1)
}

// genSeedStatements builds `world -> acc<i>` funding statements for every
// (account, asset) pair in the pool.
//
// These run at execution time, so they do NOT feed a `balance()`-origin var:
// vars-block origins resolve once, before any statement runs, on both engines.
// genPresetBalances is the only way to make such a var observe a non-zero
// value.
func genSeedStatements(rng *rand.Rand, poolSize int, assets []string) Program {
	stmts := make(Program, 0, poolSize*len(assets))
	for i := range poolSize {
		for _, asset := range assets {
			stmts = append(stmts, Statement{
				IsSendAll: false,
				Amount:    Monetary{Asset: asset, AssetAsVar: asVar(rng), Amount: seedAmount(rng)},
				Source:    Source{Kind: SrcAccount, Account: "world"},
				Destination: Destination{
					Kind:         DestAccount,
					Account:      fmt.Sprintf("acc%d", i),
					AccountAsVar: asVar(rng),
				},
			})
		}
	}
	return stmts
}

// genPresetBalances sets a starting balance, possibly negative, on a random
// subset of (account, asset) pairs, directly on the store rather than by
// executing funding statements.
//
// "world" is never included: it is never balance-backed on either engine.
func genPresetBalances(rng *rand.Rand, poolSize int, assets []string, balances map[BalanceKey]*big.Int) {
	for i := range poolSize {
		acc := fmt.Sprintf("acc%d", i)
		for _, asset := range assets {
			if rng.Intn(2) != 0 {
				continue
			}
			// [-500, 1499]: covers a plausible positive balance and a negative one.
			amount := big.NewInt(int64(rng.Intn(2000) - 500))
			balances[BalanceKey{Account: acc, Asset: asset}] = amount
		}
	}
}

// genBalances decides, per script, how accounts get their starting balances:
// `world ->` funding statements, pre-set balances on the store, or both.
//
// Weighted 50/25/25 (seeds/preset/both) rather than uniform: "both" with
// multi-asset preset balances is the shape that surfaced two real bugs, and
// seeds-only is the most-exercised path.
func genBalances(rng *rand.Rand, poolSize int, assets []string) (map[BalanceKey]*big.Int, Program) {
	balances := map[BalanceKey]*big.Int{}

	useSeeds := true
	usePreset := false
	switch rng.Intn(4) {
	case 0: // 25%: preset only
		useSeeds = false
		usePreset = true
	case 1: // 25%: both
		usePreset = true
	default: // 50%: seeds only
	}

	var seeds Program
	if useSeeds {
		seeds = genSeedStatements(rng, poolSize, assets)
	}
	if usePreset {
		genPresetBalances(rng, poolSize, assets, balances)
	}
	return balances, seeds
}

// genPresetMetadata fills the starting account metadata both engines are given,
// and records the numscript type each value is written as. meta() is typed by
// the reading declaration rather than the stored value, so genVarDecls needs
// the type to declare a var that actually parses.
func genPresetMetadata(rng *rand.Rand, poolSize int, metadata map[MetaKey]string) map[MetaKey]MetaType {
	types := map[MetaKey]MetaType{}
	for i := range poolSize {
		acc := fmt.Sprintf("acc%d", i)
		for _, key := range metaKeyPool {
			if rng.Intn(2) != 0 {
				continue
			}
			k := MetaKey{Account: acc, Key: key}
			metadata[k], types[k] = genMetaValue(rng, poolSize)
		}
	}
	return types
}

// genMetaValue produces one starting metadata value and the type it is written
// as. Numbers are the common case; the other five exist so meta() is exercised
// in every form both engines accept.
func genMetaValue(rng *rand.Rand, poolSize int) (string, MetaType) {
	switch rng.Intn(10) {
	case 0:
		return fmt.Sprintf("str%d", rng.Intn(4)), MetaString
	case 1:
		return fmt.Sprintf("%s %d", pickAsset(rng), rng.Intn(1000)), MetaMonetary
	case 2:
		return pickAsset(rng), MetaAsset
	case 3:
		return account(rng, poolSize), MetaAccount
	case 4:
		den := rng.Intn(9) + 2
		return fmt.Sprintf("%d/%d", rng.Intn(den)+1, den), MetaPortion
	default:
		return fmt.Sprintf("%d", rng.Intn(2000)-500), MetaNumber
	}
}

// metaKeyPool is to metadata keys what assetPool is to assets.
var metaKeyPool = []string{"k0", "k1", "k2"}

// genVarDecls generates 0-3 vars-block declarations, each either
// `monetary $name = balance(<account>, <asset>)` or
// `number $name = meta(<account>, "<key>")`.
//
// Accounts with a pre-set balance or metadata entry are preferred, so the var
// observes something other than the default-zero/missing-key path. "world" is
// picked at ~1 in 4 to exercise balance(@world, ASSET), which is a legal
// always-zero read on both engines.
func genVarDecls(rng *rand.Rand, poolSize int, balances map[BalanceKey]*big.Int, metadata map[MetaKey]string, metaTypes map[MetaKey]MetaType) []VarDecl {
	n := rng.Intn(4) // 0..3
	if n == 0 {
		return nil
	}

	// Both pools come from maps, whose iteration order Go randomizes. They are
	// then indexed with the seeded rng, so leaving them unsorted would make the
	// same fuzz seed generate different programs from run to run — which breaks
	// corpus replay and shrinking, the two things a saved divergence depends on.
	fundedKeys := sortedBalanceKeys(balances)
	metaKeys := make([]MetaKey, 0, len(metadata))
	for k := range metadata {
		metaKeys = append(metaKeys, k)
	}
	slices.SortFunc(metaKeys, func(a, b MetaKey) int {
		if c := cmp.Compare(a.Account, b.Account); c != 0 {
			return c
		}
		return cmp.Compare(a.Key, b.Key)
	})

	out := make([]VarDecl, n)
	for i := range out {
		if len(metaKeys) > 0 && rng.Intn(3) == 0 {
			k := metaKeys[rng.Intn(len(metaKeys))]
			out[i] = VarDecl{Kind: VarFromMeta, Account: k.Account, AccountAsVar: accountAsVar(rng, k.Account), Key: k.Key, MetaType: metaTypes[k]}
			continue
		}
		if rng.Intn(4) == 0 {
			out[i] = VarDecl{Kind: VarFromBalance, Account: "world", Asset: pickAsset(rng), AssetAsVar: asVar(rng)}
			continue
		}
		if len(fundedKeys) > 0 && rng.Intn(2) == 0 {
			k := fundedKeys[rng.Intn(len(fundedKeys))]
			out[i] = VarDecl{Kind: VarFromBalance, Account: k.Account, AccountAsVar: accountAsVar(rng, k.Account), Asset: k.Asset, AssetAsVar: asVar(rng)}
			continue
		}
		out[i] = VarDecl{Kind: VarFromBalance, Account: account(rng, poolSize), AccountAsVar: asVar(rng), Asset: pickAsset(rng), AssetAsVar: asVar(rng)}
	}

	// Bias toward the collision shape that produced two real bugs: two
	// balance()-origin vars on the same account, same or different asset. Left
	// to chance it happens only incidentally.
	if len(out) >= 2 && rng.Intn(3) == 0 {
		i := rng.Intn(len(out))
		j := rng.Intn(len(out))
		if j == i {
			j = (j + 1) % len(out)
		}
		out[j].Kind = out[i].Kind
		out[j].Account = out[i].Account
		// out[j] may have started as the other Kind, leaving Asset or Key at its
		// zero value. The "different" branch must not keep that empty string, or
		// it emits an invalid origin like balance(@acc, "") instead of the
		// intended collision.
		switch out[i].Kind {
		case VarFromBalance:
			if rng.Intn(2) == 0 {
				out[j].Asset = out[i].Asset // exact duplicate (same account+asset)
			} else if out[j].Asset == "" {
				out[j].Asset = pickAsset(rng)
			}
		case VarFromMeta:
			if rng.Intn(2) != 0 && out[j].Key != "" && out[j].Key != out[i].Key {
				// keep j's own key: a different key on the same account
			} else {
				out[j].Key = out[i].Key // exact duplicate (same account+key)
			}
			// The origin now reads a different (account, key) than j was built for,
			// so the declared type must follow the value actually stored there or
			// the script is rejected instead of compared.
			out[j].MetaType = metaTypes[MetaKey{Account: out[j].Account, Key: out[j].Key}]
		}
	}

	// Chain up to two origins onto a meta-account declaration: the account a
	// balance()/meta() call reads is itself an origin var (`balance($a, ...)`
	// with `account $a = meta(...)`). Origin-through-origin resolution is its
	// own machinery on both engines — the interpreter resolves declarations as
	// a dependency graph, the machine iterates typed resources — and the
	// builder toposorts the declarations, so the shape is generated
	// deliberately rather than left to chance. A chained VarFromMeta landing
	// on an account-typed value is itself a parent candidate, so the second
	// pass can chain one level deeper. Appended after the aliasing bias above
	// so it never rewrites a chained declaration.
	for range 2 {
		if rng.Intn(3) != 0 {
			continue
		}
		parents := metaAccountVarIndices(out)
		if len(parents) == 0 {
			// Synthesize one: a fresh meta key (outside metaKeyPool, so no
			// other declaration's expectations shift) preset to an account
			// name.
			acc := account(rng, poolSize)
			mk := MetaKey{Account: acc, Key: fmt.Sprintf("chain%d", len(out))}
			metadata[mk] = account(rng, poolSize)
			metaTypes[mk] = MetaAccount
			out = append(out, VarDecl{Kind: VarFromMeta, Account: acc, AccountAsVar: accountAsVar(rng, acc), Key: mk.Key, MetaType: MetaAccount})
			parents = []int{len(out) - 1}
		}
		p := parents[rng.Intn(len(parents))]
		target := metadata[MetaKey{Account: out[p].Account, Key: out[p].Key}]
		idx := p
		if rng.Intn(2) == 0 {
			// balance($parent, <asset>), preferring an asset whose preset
			// balance is non-negative: both engines reject a negative
			// balance() read outright, which would compare nothing.
			asset := pickAsset(rng)
			for range assetPool {
				preset, ok := balances[BalanceKey{Account: target, Asset: asset}]
				if !ok || preset.Sign() >= 0 {
					break
				}
				asset = pickAsset(rng)
			}
			out = append(out, VarDecl{Kind: VarFromBalance, Account: target, AccountFromVarIdx: &idx, Asset: asset, AssetAsVar: asVar(rng)})
			continue
		}
		// meta($parent, "<key>") needs a preset (target, key): a missing-key
		// read fails on both engines and compares nothing.
		var keys []string
		for _, k := range metaKeyPool {
			if _, ok := metadata[MetaKey{Account: target, Key: k}]; ok {
				keys = append(keys, k)
			}
		}
		if len(keys) == 0 {
			continue
		}
		key := keys[rng.Intn(len(keys))]
		out = append(out, VarDecl{Kind: VarFromMeta, Account: target, AccountFromVarIdx: &idx, Key: key, MetaType: metaTypes[MetaKey{Account: target, Key: key}]})
	}

	return out
}

// metaAccountVarIndices returns the indices of declarations usable as a
// chained origin's account: VarFromMeta declarations whose stored value is an
// account name.
func metaAccountVarIndices(vars []VarDecl) []int {
	var idxs []int
	for i, v := range vars {
		if v.Kind == VarFromMeta && v.MetaType == MetaAccount {
			idxs = append(idxs, i)
		}
	}
	return idxs
}

// genNumExpr generates a small arithmetic expression for
// set_tx_meta/set_account_meta values. depth bounds recursion.
//
// Leaf literals are never negative: the grammar has no unary minus, so a
// negative leaf would make the oracle reject every such script, which Compare
// tolerates and which is therefore no coverage at all. NumSub can still produce
// a negative value at run time.
func genNumExpr(rng *rand.Rand, depth int) NumExpr {
	if depth <= 0 || rng.Intn(3) != 0 {
		return NumExpr{Kind: NumLit, Lit: big.NewInt(int64(rng.Intn(1000))), LitAsVar: asVar(rng)}
	}
	left := genNumExpr(rng, depth-1)
	right := genNumExpr(rng, depth-1)
	if rng.Intn(2) == 0 {
		return NumExpr{Kind: NumAdd, Left: &left, Right: &right}
	}
	return NumExpr{Kind: NumSub, Left: &left, Right: &right}
}

// genMetaStringValue occasionally makes a meta value a string instead of a
// number, so string-typed values are exercised in both their inline and var
// forms. nil means "keep the numeric expression".
func genMetaStringValue(rng *rand.Rand) (*string, bool) {
	if rng.Intn(3) != 0 {
		return nil, false
	}
	s := fmt.Sprintf("str%d", rng.Intn(4))
	return &s, asVar(rng)
}

// varIndicesOfKind returns the indices into vars whose Kind matches.
func varIndicesOfKind(vars []VarDecl, kind VarDeclKind) []int {
	var idxs []int
	for i, v := range vars {
		if v.Kind == kind {
			idxs = append(idxs, i)
		}
	}
	return idxs
}

// genAccountVarDecls generates 0-2 plain (runtime-fed) account-typed vars.
// Value is "world" at a deliberate ~1-in-3 rate — see AccountVarDecl's doc
// comment for why.
func genAccountVarDecls(rng *rand.Rand, poolSize int) []AccountVarDecl {
	n := rng.Intn(3) // 0..2
	out := make([]AccountVarDecl, n)
	for i := range out {
		if rng.Intn(3) == 0 {
			out[i] = AccountVarDecl{Value: "world"}
		} else {
			out[i] = AccountVarDecl{Value: account(rng, poolSize)}
		}
	}
	return out
}

// genExtraStatements generates 0-3 non-send statements (save/set_tx_meta/
// set_account_meta/a var-backed send/a meta()-var-backed set_tx_meta/a
// send through an account-typed var), interspersed with the core send-only
// program by the caller (see riffleOrder).
func genExtraStatements(rng *rand.Rand, poolSize int, vars []VarDecl, accountVars []AccountVarDecl) []ExtraStatement {
	n := rng.Intn(4) // 0..3
	out := make([]ExtraStatement, 0, n)

	balanceVarIdxs := varIndicesOfKind(vars, VarFromBalance)
	metaVarIdxs := varIndicesOfKind(vars, VarFromMeta)

	for range n {
		kind := ExtraStatementKind(rng.Intn(8))
		// ExtraSave/ExtraSendVar need a declared balance()-origin var to
		// reference; ExtraSetTxMetaVar needs a declared meta()-origin var;
		// ExtraSendFromAccountVar/ExtraSendToAccountVar need a declared
		// account-typed var. Fall back to a plain set_tx_meta if the
		// needed kind isn't available.
		if (kind == ExtraSave && rng.Intn(2) == 0 || kind == ExtraSendVar) && len(balanceVarIdxs) == 0 {
			kind = ExtraSetTxMeta
		}
		if kind == ExtraSetTxMetaVar && len(metaVarIdxs) == 0 {
			kind = ExtraSetTxMeta
		}
		if (kind == ExtraSendFromAccountVar || kind == ExtraSendToAccountVar) && len(accountVars) == 0 {
			kind = ExtraSetTxMeta
		}

		switch kind {
		case ExtraSave:
			acc := account(rng, poolSize)
			if len(balanceVarIdxs) > 0 && rng.Intn(2) == 0 {
				idx := balanceVarIdxs[rng.Intn(len(balanceVarIdxs))]
				out = append(out, ExtraStatement{Kind: ExtraSave, VarIdx: &idx, Account: acc, AccountAsVar: asVar(rng)})
			} else {
				asset := pickAsset(rng)
				m := monetary(rng, asset)
				out = append(out, ExtraStatement{Kind: ExtraSave, Monetary: &m, Account: acc, AccountAsVar: asVar(rng)})
			}

		case ExtraSaveAll:
			out = append(out, ExtraStatement{
				Kind:       ExtraSaveAll,
				Asset:      pickAsset(rng),
				AssetAsVar: asVar(rng),
				Account:    account(rng, poolSize), AccountAsVar: asVar(rng),
			})

		case ExtraSetTxMeta:
			sv, svVar := genMetaStringValue(rng)
			out = append(out, ExtraStatement{
				Kind:             ExtraSetTxMeta,
				Key:              fmt.Sprintf("k%d", rng.Intn(5)),
				Value:            genNumExpr(rng, 3),
				StringValue:      sv,
				StringValueAsVar: svVar,
			})

		case ExtraSetAccountMeta:
			sv, svVar := genMetaStringValue(rng)
			out = append(out, ExtraStatement{
				Kind:    ExtraSetAccountMeta,
				Account: account(rng, poolSize), AccountAsVar: asVar(rng),
				Key:              fmt.Sprintf("k%d", rng.Intn(5)),
				Value:            genNumExpr(rng, 3),
				StringValue:      sv,
				StringValueAsVar: svVar,
			})

		case ExtraSendVar:
			idx := balanceVarIdxs[rng.Intn(len(balanceVarIdxs))]
			out = append(out, ExtraStatement{
				Kind:    ExtraSendVar,
				VarIdx:  &idx,
				Account: account(rng, poolSize), AccountAsVar: asVar(rng),
				Destination:      account(rng, poolSize),
				DestinationAsVar: asVar(rng),
			})

		case ExtraSetTxMetaVar:
			idx := metaVarIdxs[rng.Intn(len(metaVarIdxs))]
			out = append(out, ExtraStatement{
				Kind:   ExtraSetTxMetaVar,
				VarIdx: &idx,
				Key:    fmt.Sprintf("k%d", rng.Intn(5)),
			})

		case ExtraSendFromAccountVar:
			idx := rng.Intn(len(accountVars))
			asset := pickAsset(rng)
			m := monetary(rng, asset)
			out = append(out, ExtraStatement{
				Kind:          ExtraSendFromAccountVar,
				AccountVarIdx: &idx,
				Monetary:      &m,
				Account:       account(rng, poolSize), AccountAsVar: asVar(rng), // destination
			})

		case ExtraSendToAccountVar:
			idx := rng.Intn(len(accountVars))
			asset := pickAsset(rng)
			m := monetary(rng, asset)
			out = append(out, ExtraStatement{
				Kind:          ExtraSendToAccountVar,
				AccountVarIdx: &idx,
				Monetary:      &m,
				Account:       account(rng, poolSize), AccountAsVar: asVar(rng), // source
			})
		}
	}

	// Two balance() origins on one account only expose the resource-aliasing bug
	// (formancehq/ledger#2056) if BOTH are referenced: an unreferenced origin var
	// is never declared. Leaving that to chance put the full shape at roughly 1
	// script in 30,000, so reference both deliberately.
	if i, j, ok := aliasedBalanceVarPair(vars); ok && rng.Intn(2) == 0 {
		for _, idx := range [2]int{i, j} {
			v := idx
			out = append(out, ExtraStatement{
				Kind:    ExtraSendVar,
				VarIdx:  &v,
				Account: account(rng, poolSize), AccountAsVar: asVar(rng),
				Destination:      account(rng, poolSize),
				DestinationAsVar: asVar(rng),
			})
		}
	}

	// A chained origin only exercises origin-through-origin resolution if the
	// chained var is referenced (an unreferenced origin var is never
	// declared). The kinds above pick their var uniformly, which would leave
	// most chains unreferenced, so reference each one deliberately.
	for i, v := range vars {
		if v.AccountFromVarIdx == nil || rng.Intn(2) != 0 {
			continue
		}
		idx := i
		if v.Kind == VarFromBalance {
			out = append(out, ExtraStatement{
				Kind:    ExtraSendVar,
				VarIdx:  &idx,
				Account: account(rng, poolSize), AccountAsVar: asVar(rng),
				Destination:      account(rng, poolSize),
				DestinationAsVar: asVar(rng),
			})
		} else {
			out = append(out, ExtraStatement{Kind: ExtraSetTxMetaVar, VarIdx: &idx, Key: fmt.Sprintf("k%d", rng.Intn(5))})
		}
	}

	return out
}

// riffleOrder returns a random interleaving of two sequences of lengths a and
// b (true = take the next element from the first), so Extra statements land
// throughout Program instead of only after it.
func riffleOrder(rng *rand.Rand, a, b int) []bool {
	order := make([]bool, 0, a+b)
	for a > 0 && b > 0 {
		if rng.Intn(a+b) < a {
			order = append(order, true)
			a--
		} else {
			order = append(order, false)
			b--
		}
	}
	for ; a > 0; a-- {
		order = append(order, true)
	}
	for ; b > 0; b-- {
		order = append(order, false)
	}
	return order
}

// generateScriptAST runs one full round of generation: account-pool size, how
// balances and metadata are seeded, the vars block, the send-only program, the
// extra non-send statements, how the last two interleave, and finally which
// Strategy the body uses.
//
// The strategy is drawn last, after every draw the uniform path makes, so a
// seed that lands on StrategyUniform produces the same script it did before
// scenarios existed. StrategyScenarioOnly still runs genProgram and
// genExtraStatements for the same reason, and drops their output.
func generateScriptAST(rng *rand.Rand) Script {
	poolSize := pickPoolSize(rng)
	// Decided per script, not per occurrence: most scripts must stay inside the
	// oracle's grammar or the oracle legs lose their corpus. A quarter may use
	// numscript-only shapes (oneof, colors, wrong-asset caps, division
	// portions); those that actually do skip the oracle — see scriptFlags.
	numscriptOnly := rng.Intn(4) == 0

	balances, seeds := genBalances(rng, poolSize, assetPool)
	metadata := map[MetaKey]string{}
	metaTypes := genPresetMetadata(rng, poolSize, metadata)
	vars := genVarDecls(rng, poolSize, balances, metadata, metaTypes)
	accountVars := genAccountVarDecls(rng, poolSize)
	program := cleanupProgram(genProgram(rng, poolSize, numscriptOnly))
	extra := genExtraStatements(rng, poolSize, vars, accountVars)
	order := riffleOrder(rng, len(program), len(extra))

	s := Script{
		Vars:        vars,
		AccountVars: accountVars,
		Seeds:       seeds,
		Program:     program,
		Extra:       extra,
		Order:       order,
		Strategy:    StrategyUniform,
		Balances:    balances,
		Metadata:    metadata,
	}

	switch rng.Intn(4) {
	case 0, 1:
	case 2:
		s.Strategy = StrategyScenarioMixed
		var focus Focus
		s.Scenario, s.ScenarioPos, focus = genScenario(rng, poolSize, seeds, balances, len(order))
		s.Focus = &focus
	case 3:
		s.Strategy = StrategyScenarioOnly
		s.Program, s.Extra, s.Order = nil, nil, nil
		var focus Focus
		s.Scenario, s.ScenarioPos, focus = genScenario(rng, poolSize, seeds, balances, 0)
		s.Focus = &focus
	}
	return s
}

// aliasedBalanceVarPair returns two distinct VarFromBalance indices whose
// declarations read the balance of the same account, if any exist.
func aliasedBalanceVarPair(vars []VarDecl) (int, int, bool) {
	firstByAccount := map[string]int{}
	for i, v := range vars {
		if v.Kind != VarFromBalance {
			continue
		}
		if j, seen := firstByAccount[v.Account]; seen {
			return j, i, true
		}
		firstByAccount[v.Account] = i
	}
	return 0, 0, false
}
