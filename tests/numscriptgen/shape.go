package numscriptgen

import (
	"math/big"
	"slices"
	"strings"
)

// Shape records structural facts about one Script, so a sweep can count how
// often the multi-statement shapes it cares about are actually generated.
// Everything here is read off the AST and the starting balances; no engine
// semantics are involved.
type Shape struct {
	Strategy Strategy

	HasSave             bool
	HasBoundedOverdraft bool
	// A save and a bounded-overdraft source on the same account, any assets.
	SaveOverdraftSameAccount bool
	// Same, on the same (account, asset).
	SaveOverdraftSameResource bool
	// Same, with the save executing before the overdraft draw.
	SaveOverdraftSameResourceInOrder bool
	// An in-order pair whose literal save amount exceeds the pair's starting
	// balance (preset plus seed funding), with no earlier statement touching
	// the pair. A var-valued or `*` save never counts.
	SaveOverdrawsInitial bool

	// An allotment (source or destination side) with a `remaining` clause.
	HasAllotmentRemaining bool
	// An allotment clause whose portion is a var rather than an n/d literal.
	HasPortionVar bool
	// A statement references a vars-block origin that reads through another
	// origin var (VarDecl.AccountFromVarIdx). Counted on references, not
	// declarations: an unreferenced origin var is never rendered into the
	// script.
	HasChainedOrigin bool

	// Shapes outside numscript's core: the first two need feature flags.
	HasOneof         bool
	HasColoredSource bool
	HasWrongAssetCap bool
	HasPortionDiv    bool

	// Scenario kinds in block order, empty without a scenario block.
	SetupKinds    []string
	ObserverKinds []string
}

// bodyStmt is one statement of a Script's body, after Seeds. Exactly one
// field is set.
type bodyStmt struct {
	send  *Statement
	extra *ExtraStatement
}

// bodyStatements returns Program, Extra and Scenario in execution order.
func bodyStatements(s Script) []bodyStmt {
	out := make([]bodyStmt, 0, len(s.Order)+len(s.Scenario))
	scenario := func() {
		for i := range s.Scenario {
			out = append(out, bodyStmt{send: s.Scenario[i].Send, extra: s.Scenario[i].Extra})
		}
	}

	pi, ei := 0, 0
	for i, takeProgram := range s.Order {
		if i == s.ScenarioPos {
			scenario()
		}
		if takeProgram {
			out = append(out, bodyStmt{send: &s.Program[pi]})
			pi++
		} else {
			out = append(out, bodyStmt{extra: &s.Extra[ei]})
			ei++
		}
	}
	if s.ScenarioPos >= len(s.Order) {
		scenario()
	}
	return out
}

// Scenario kinds are named "setup:<name>" or "observe:<name>".
func isObserverKind(kind string) bool {
	return strings.HasPrefix(kind, "observe:")
}

type saveFact struct {
	index   int
	account string
	asset   string
	// nil for a var-valued or `*` save
	amount *big.Int
}

type overdraftFact struct {
	index   int
	account string
	asset   string
}

func computeShape(s Script) Shape {
	sh := Shape{Strategy: s.Strategy}
	for _, st := range s.Scenario {
		if isObserverKind(st.Kind) {
			sh.ObserverKinds = append(sh.ObserverKinds, st.Kind)
		} else {
			sh.SetupKinds = append(sh.SetupKinds, st.Kind)
		}
	}

	stmts := bodyStatements(s)
	var saves []saveFact
	var overdrafts []overdraftFact
	for i, st := range stmts {
		if st.extra != nil {
			if f, ok := saveOf(i, *st.extra, s.Vars); ok {
				saves = append(saves, f)
			}
			if st.extra.VarIdx != nil && s.Vars[*st.extra.VarIdx].AccountFromVarIdx != nil {
				sh.HasChainedOrigin = true
			}
			continue
		}
		asset := st.send.Amount.Asset
		if st.send.IsSendAll {
			asset = st.send.Asset
		}
		for _, acc := range boundedOverdraftAccounts(st.send.Source, nil) {
			overdrafts = append(overdrafts, overdraftFact{index: i, account: acc, asset: asset})
		}
		srcRem, srcVar := allotmentShapeSrc(st.send.Source)
		destRem, destVar := allotmentShapeDest(st.send.Destination)
		sh.HasAllotmentRemaining = sh.HasAllotmentRemaining || srcRem || destRem
		sh.HasPortionVar = sh.HasPortionVar || srcVar || destVar
		extendedFactsSrc(st.send.Source, asset, &sh)
		extendedFactsDest(st.send.Destination, asset, &sh)
	}
	sh.HasSave = len(saves) > 0
	sh.HasBoundedOverdraft = len(overdrafts) > 0

	for _, sv := range saves {
		for _, od := range overdrafts {
			if sv.account != od.account {
				continue
			}
			sh.SaveOverdraftSameAccount = true
			if sv.asset != od.asset {
				continue
			}
			sh.SaveOverdraftSameResource = true
			if sv.index >= od.index {
				continue
			}
			sh.SaveOverdraftSameResourceInOrder = true
			if sv.amount == nil {
				continue
			}
			pair := BalanceKey{Account: sv.account, Asset: sv.asset}
			if sv.amount.Cmp(initialBalance(s, pair)) > 0 && !touchedBefore(stmts[:sv.index], pair, s) {
				sh.SaveOverdrawsInitial = true
			}
		}
	}
	return sh
}

func saveOf(index int, e ExtraStatement, vars []VarDecl) (saveFact, bool) {
	switch e.Kind {
	case ExtraSave:
		f := saveFact{index: index, account: e.Account}
		if e.VarIdx != nil {
			f.asset = vars[*e.VarIdx].Asset
		} else {
			f.asset = e.Monetary.Asset
			f.amount = e.Monetary.Amount
		}
		return f, true
	case ExtraSaveAll:
		return saveFact{index: index, account: e.Account, asset: e.Asset}, true
	default:
		return saveFact{}, false
	}
}

// extendedFactsSrc records the shapes outside numscript's core present in one
// source tree; asset is the statement's asset, for wrong-asset cap detection.
func extendedFactsSrc(src Source, asset string, sh *Shape) {
	switch src.Kind {
	case SrcAccount:
		sh.HasColoredSource = sh.HasColoredSource || src.Color != ""
	case SrcAccountOverdraft:
		sh.HasColoredSource = sh.HasColoredSource || src.Color != ""
		if src.Overdraft != nil && src.Overdraft.Asset != asset {
			sh.HasWrongAssetCap = true
		}
	case SrcCapped:
		if src.Cap.Asset != asset {
			sh.HasWrongAssetCap = true
		}
		extendedFactsSrc(*src.Inner, asset, sh)
	case SrcInorder, SrcOneof:
		sh.HasOneof = sh.HasOneof || src.Kind == SrcOneof
		for _, inner := range src.Sources {
			extendedFactsSrc(inner, asset, sh)
		}
	case SrcAllotment:
		for _, c := range src.Clauses {
			sh.HasPortionDiv = sh.HasPortionDiv || c.Div != nil
			extendedFactsSrc(c.Source, asset, sh)
		}
		if src.AllotmentRemaining != nil {
			extendedFactsSrc(*src.AllotmentRemaining, asset, sh)
		}
	}
}

func extendedFactsDest(d Destination, asset string, sh *Shape) {
	keptOrDest := func(k KeptOrDest) {
		if k.Kind == To {
			extendedFactsDest(*k.Dest, asset, sh)
		}
	}
	switch d.Kind {
	case DestInorder, DestOneof:
		sh.HasOneof = sh.HasOneof || d.Kind == DestOneof
		for _, c := range d.InorderClauses {
			if c.Max.Asset != asset {
				sh.HasWrongAssetCap = true
			}
			keptOrDest(c.KeptOrDest)
		}
		if d.Remaining != nil {
			keptOrDest(*d.Remaining)
		}
	case DestAllotment:
		for _, c := range d.AllotClauses {
			sh.HasPortionDiv = sh.HasPortionDiv || c.Div != nil
			keptOrDest(c.KeptOrDest)
		}
		if d.AllotRemaining != nil {
			keptOrDest(*d.AllotRemaining)
		}
	}
}

func boundedOverdraftAccounts(src Source, acc []string) []string {
	switch src.Kind {
	case SrcAccountOverdraft:
		if src.Overdraft != nil {
			acc = append(acc, src.Account)
		}
	case SrcCapped:
		acc = boundedOverdraftAccounts(*src.Inner, acc)
	case SrcInorder, SrcOneof:
		for _, inner := range src.Sources {
			acc = boundedOverdraftAccounts(inner, acc)
		}
	case SrcAllotment:
		for _, c := range src.Clauses {
			acc = boundedOverdraftAccounts(c.Source, acc)
		}
		if src.AllotmentRemaining != nil {
			acc = boundedOverdraftAccounts(*src.AllotmentRemaining, acc)
		}
	}
	return acc
}

// initialBalance is the balance both engines start a pair at: the preset
// value plus every seed funding statement into it.
func initialBalance(s Script, pair BalanceKey) *big.Int {
	b := new(big.Int)
	if preset, ok := s.Balances[pair]; ok {
		b.Set(preset)
	}
	for _, seed := range s.Seeds {
		if seed.Destination.Account == pair.Account && seed.Amount.Asset == pair.Asset {
			b.Add(b, seed.Amount.Amount)
		}
	}
	return b
}

// touchedBefore reports whether any of stmts references pair as a source,
// destination or save target.
func touchedBefore(stmts []bodyStmt, pair BalanceKey, s Script) bool {
	for _, st := range stmts {
		if slices.Contains(touchedPairs(st, s), pair) {
			return true
		}
	}
	return false
}

func touchedPairs(st bodyStmt, s Script) []BalanceKey {
	if st.send != nil {
		asset := st.send.Amount.Asset
		if st.send.IsSendAll {
			asset = st.send.Asset
		}
		var out []BalanceKey
		for _, acc := range sourceAccounts(st.send.Source, nil) {
			out = append(out, BalanceKey{Account: acc, Asset: asset})
		}
		for _, acc := range destinationAccounts(st.send.Destination, nil) {
			out = append(out, BalanceKey{Account: acc, Asset: asset})
		}
		return out
	}

	e := st.extra
	switch e.Kind {
	case ExtraSave:
		if f, ok := saveOf(0, *e, s.Vars); ok {
			return []BalanceKey{{Account: f.account, Asset: f.asset}}
		}
	case ExtraSaveAll:
		return []BalanceKey{{Account: e.Account, Asset: e.Asset}}
	case ExtraSendVar:
		asset := s.Vars[*e.VarIdx].Asset
		return []BalanceKey{{Account: e.Account, Asset: asset}, {Account: e.Destination, Asset: asset}}
	case ExtraSendFromAccountVar:
		return []BalanceKey{
			{Account: s.AccountVars[*e.AccountVarIdx].Value, Asset: e.Monetary.Asset},
			{Account: e.Account, Asset: e.Monetary.Asset},
		}
	case ExtraSendToAccountVar:
		return []BalanceKey{
			{Account: e.Account, Asset: e.Monetary.Asset},
			{Account: s.AccountVars[*e.AccountVarIdx].Value, Asset: e.Monetary.Asset},
		}
	}
	return nil
}

// allotmentShapeSrc reports (has a remaining clause, has a portion var) over
// one source tree.
func allotmentShapeSrc(src Source) (bool, bool) {
	remaining, portionVar := false, false
	merge := func(r, v bool) { remaining, portionVar = remaining || r, portionVar || v }
	switch src.Kind {
	case SrcCapped:
		merge(allotmentShapeSrc(*src.Inner))
	case SrcInorder, SrcOneof:
		for _, inner := range src.Sources {
			merge(allotmentShapeSrc(inner))
		}
	case SrcAllotment:
		remaining = src.AllotmentRemaining != nil
		for _, c := range src.Clauses {
			portionVar = portionVar || c.PortionAsVar
			merge(allotmentShapeSrc(c.Source))
		}
		if src.AllotmentRemaining != nil {
			merge(allotmentShapeSrc(*src.AllotmentRemaining))
		}
	}
	return remaining, portionVar
}

func allotmentShapeDest(d Destination) (bool, bool) {
	remaining, portionVar := false, false
	merge := func(r, v bool) { remaining, portionVar = remaining || r, portionVar || v }
	keptOrDest := func(k KeptOrDest) {
		if k.Kind == To {
			merge(allotmentShapeDest(*k.Dest))
		}
	}
	switch d.Kind {
	case DestInorder, DestOneof:
		for _, c := range d.InorderClauses {
			keptOrDest(c.KeptOrDest)
		}
		if d.Remaining != nil {
			keptOrDest(*d.Remaining)
		}
	case DestAllotment:
		remaining = d.AllotRemaining != nil
		for _, c := range d.AllotClauses {
			portionVar = portionVar || c.PortionAsVar
			keptOrDest(c.KeptOrDest)
		}
		if d.AllotRemaining != nil {
			keptOrDest(*d.AllotRemaining)
		}
	}
	return remaining, portionVar
}

func sourceAccounts(src Source, acc []string) []string {
	switch src.Kind {
	case SrcAccount, SrcAccountOverdraft:
		acc = append(acc, src.Account)
	case SrcCapped:
		acc = sourceAccounts(*src.Inner, acc)
	case SrcInorder, SrcOneof:
		for _, inner := range src.Sources {
			acc = sourceAccounts(inner, acc)
		}
	case SrcAllotment:
		for _, c := range src.Clauses {
			acc = sourceAccounts(c.Source, acc)
		}
		if src.AllotmentRemaining != nil {
			acc = sourceAccounts(*src.AllotmentRemaining, acc)
		}
	}
	return acc
}

func destinationAccounts(d Destination, acc []string) []string {
	switch d.Kind {
	case DestAccount:
		acc = append(acc, d.Account)
	case DestInorder, DestOneof:
		for _, c := range d.InorderClauses {
			acc = keptOrDestAccounts(c.KeptOrDest, acc)
		}
		if d.Remaining != nil {
			acc = keptOrDestAccounts(*d.Remaining, acc)
		}
	case DestAllotment:
		for _, c := range d.AllotClauses {
			acc = keptOrDestAccounts(c.KeptOrDest, acc)
		}
		if d.AllotRemaining != nil {
			acc = keptOrDestAccounts(*d.AllotRemaining, acc)
		}
	}
	return acc
}

func keptOrDestAccounts(k KeptOrDest, acc []string) []string {
	if k.Kind == To {
		return destinationAccounts(*k.Dest, acc)
	}
	return acc
}
