package main

import (
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"sort"
	"strings"
	"sync"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/pkg/semver"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/numscriptgen"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Numscript generation. Scripts come from numscriptgen over the model's own
// address pool, so they read and move the balances the rest of the workload
// builds. Like every other generator here, nothing is expected of a script:
// the oracle runs it and decides what the server owes.

// numscriptNames is the library's name pool: small, so saves pile versions onto
// the same names and collide on existing ones.
var numscriptNames = []string{"ns0", "ns1", "ns2", "ns3", "ns4"}

// scriptCatalog remembers, per script text, what the generator produced it
// from — its runtime vars and the accounts it was drawn over — so a later
// reference to a saved version gets vars that fit the program. It only shapes
// requests; validation reads the stored script from the model.
var scriptCatalog = struct {
	sync.Mutex
	byText map[string]catalogEntry
}{byText: map[string]catalogEntry{}}

type catalogEntry struct {
	vars     map[string]string
	accounts []string
}

const scriptCatalogCap = 1024

func rememberScript(text string, vars map[string]string, accounts []string) {
	scriptCatalog.Lock()
	defer scriptCatalog.Unlock()

	if len(scriptCatalog.byText) >= scriptCatalogCap {
		clear(scriptCatalog.byText)
	}

	scriptCatalog.byText[text] = catalogEntry{vars: vars, accounts: accounts}
}

func recallScript(text string) (catalogEntry, bool) {
	scriptCatalog.Lock()
	defer scriptCatalog.Unlock()

	e, ok := scriptCatalog.byText[text]

	return e, ok
}

// scriptAccounts draws the addresses a script's pool accounts stand for.
// Repeats alias two pool accounts onto one address.
func scriptAccounts() []string {
	out := make([]string, 15)
	for i := range out {
		out[i] = poolAddress()
	}

	return out
}

func rollNumscriptBulk() bool {
	return oneIn(8)
}

// generateNumscriptBulk builds a single-ledger bulk around the library or a
// script: an inline script, a save, a reference to a saved version, or a
// malformed create.
func generateNumscriptBulk(g oracle.GlobalState, ledger string) []*servicepb.Request {
	ls := g.Ledger(ledger)

	switch random.RandomChoice(indexPool(10)) {
	case 0, 1, 2, 3:
		gen, accounts := generateScript()

		return append(seedScriptInputs(ledger, ls, gen), maybeAddSkippableReason(ls, scriptedCreate(ledger, inlineScript(gen), accounts)))
	case 4, 5:
		if req := referenceCreate(ledger, ls); req != nil {
			return []*servicepb.Request{maybeAddSkippableReason(ls, req)}
		}

		return nil
	case 6:
		return []*servicepb.Request{saveNumscript(ledger, ls)}
	case 7:
		save := saveNumscript(ledger, ls)
		r := save.GetSaveNumscript()
		ref := &servicepb.ScriptReference{Name: r.GetName(), Version: "latest"}
		if oneIn(2) {
			ref.Version = r.GetVersion()
		}

		if e, ok := recallScript(r.GetContent()); ok {
			ref.Vars = e.vars
		}

		return []*servicepb.Request{save, scriptedCreate(ledger, ref, nil)}
	default:
		return []*servicepb.Request{malformedScriptCreate(ledger)}
	}
}

func generateScript() (numscriptgen.Generated, []string) {
	accounts := scriptAccounts()
	gen := numscriptgen.GenerateFor(internal.Rand(), accounts)
	rememberScript(gen.Script, gen.Vars, accounts)

	return gen, accounts
}

// inlineScript wraps a generated program as a create's inline script, rarely
// broken: truncated text, a dropped var, or a var bound to garbage.
func inlineScript(gen numscriptgen.Generated) *commonpb.Script {
	script := &commonpb.Script{Plain: gen.Script, Vars: maps.Clone(gen.Vars)}
	if oneIn(16) {
		mutateScript(script)
	}

	return script
}

func mutateScript(script *commonpb.Script) {
	names := slices.Sorted(maps.Keys(script.Vars))

	switch random.RandomChoice(indexPool(3)) {
	case 0:
		if len(script.Plain) > 1 {
			script.Plain = script.Plain[:1+internal.Rand().Uint64()%uint64(len(script.Plain)-1)]
		}
	case 1:
		if len(names) > 0 {
			delete(script.Vars, random.RandomChoice(names))
		}
	default:
		if len(names) > 0 {
			script.Vars[random.RandomChoice(names)] = random.RandomChoice([]string{"", "?!", "-1", "@world", "USD/2"})
		}
	}
}

// seedScriptInputs is the bulk prefix that moves the ledger toward the starting
// state the generator built the script for: one forced create moving each
// (account, asset) balance to its preset value against world, then the preset
// account metadata. Balances are read from a published snapshot, so concurrent
// bulks can leave them off target — the script then just runs on other inputs.
// A quarter of scripts run on the ledger as it stands.
func seedScriptInputs(ledger string, ls oracle.LedgerState, gen numscriptgen.Generated) []*servicepb.Request {
	if oneIn(4) {
		return nil
	}

	var reqs []*servicepb.Request

	keys := slices.SortedFunc(maps.Keys(gen.Balances), func(a, b numscriptgen.BalanceKey) int {
		return strings.Compare(a.Account+"\x00"+a.Asset, b.Account+"\x00"+b.Asset)
	})

	var postings []*commonpb.Posting
	for _, k := range keys {
		vp, _ := ls.Volumes().Get(oracle.VolumeKey{Address: k.Account, Asset: k.Asset})
		current := new(big.Int).Sub(vp.Input.ToBig(), vp.Output.ToBig())
		delta := new(big.Int).Sub(gen.Balances[k], current)

		var amount uint256.Int
		switch delta.Sign() {
		case 1:
			if amount.SetFromBig(delta) {
				continue
			}

			postings = append(postings, commonpb.NewPosting("world", k.Account, k.Asset, amount.ToBig()))
		case -1:
			if amount.SetFromBig(new(big.Int).Neg(delta)) {
				continue
			}

			postings = append(postings, commonpb.NewPosting(k.Account, "world", k.Asset, amount.ToBig()))
		}
	}

	if len(postings) > 0 {
		reqs = append(reqs, applyCreate(ledger, &servicepb.CreateTransactionPayload{
			Postings:  postings,
			Reference: txRef(),
			Force:     true,
		}))
	}

	byAccount := map[string]map[string]*commonpb.MetadataValue{}
	for k, v := range gen.Metadata {
		if byAccount[k.Account] == nil {
			byAccount[k.Account] = map[string]*commonpb.MetadataValue{}
		}

		byAccount[k.Account][k.Key] = commonpb.NewStringValue(v)
	}

	for _, addr := range slices.Sorted(maps.Keys(byAccount)) {
		reqs = append(reqs, accountMetadataRequest(ledger, addr, byAccount[addr]))
	}

	return reqs
}

func accountMetadataRequest(ledger, addr string, md map[string]*commonpb.MetadataValue) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_Apply{Apply: &servicepb.LedgerApplyRequest{
		Ledger: ledger,
		Action: &servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{
			AddMetadata: &commonpb.SaveMetadataCommand{Target: accountTarget(addr), Metadata: md},
		}},
	}}}
}

// scriptedCreate wraps a script (a *commonpb.Script or a reference) in a create
// shaped like the ordinary ones. Caller metadata sometimes reuses the keys
// scripts write (k0..k4), so the caller-wins merge is exercised.
func scriptedCreate(ledger string, script any, accounts []string) *servicepb.Request {
	payload := &servicepb.CreateTransactionPayload{
		Reference: txRef(),
		Force:     oneIn(8),
	}

	switch s := script.(type) {
	case *commonpb.Script:
		payload.Script = s
	case *servicepb.ScriptReference:
		payload.ScriptReference = s
	}

	if oneIn(4) {
		payload.Metadata = scriptMetaMap()
	}

	if oneIn(2) {
		payload.Timestamp = &commonpb.Timestamp{Data: 1 + internal.Rand().Uint64()%1_500_000_000_000_000}
	}

	if len(accounts) > 0 && oneIn(8) {
		payload.AccountMetadata = map[string]*commonpb.MetadataMap{
			random.RandomChoice(accounts): {Values: scriptMetaMap()},
		}
	}

	return applyCreate(ledger, payload)
}

func scriptMetaMap() map[string]*commonpb.MetadataValue {
	md := randomMetaMap()
	md[fmt.Sprintf("k%d", internal.Rand().Uint64()%5)] = commonpb.NewStringValue("caller")

	return md
}

// referenceCreate runs a library script: usually the latest or an exact stored
// version of a name the ledger has, sometimes an unknown version or a partial
// selector (refused at admission). Vars are the ones the text was generated
// with, half the time redrawn.
func referenceCreate(ledger string, ls oracle.LedgerState) *servicepb.Request {
	name := random.RandomChoice(numscriptNames)
	versions := storedVersions(ls, name)

	selector := "latest"
	switch {
	case oneIn(16):
		selector = random.RandomChoice([]string{"1", "1.2", "9.9.9", ""})
	case len(versions) > 0 && oneIn(3):
		selector = random.RandomChoice(versions)
	}

	resolved := selector
	if selector == "latest" {
		resolved = ls.NumscriptLatest(name)
	}

	ref := &servicepb.ScriptReference{Name: name, Version: selector}

	var accounts []string
	if content, ok := ls.NumscriptContent(name, resolved); ok {
		if e, ok := recallScript(content); ok {
			accounts = e.accounts
			ref.Vars = e.vars
			if oneIn(2) {
				ref.Vars = numscriptgen.RerollVars(internal.Rand(), e.vars, e.accounts)
			}
		}
	}

	return scriptedCreate(ledger, ref, accounts)
}

func storedVersions(ls oracle.LedgerState, name string) []string {
	var out []string
	for k := range ls.Numscripts() {
		if k.Name == name {
			out = append(out, k.Version)
		}
	}

	sort.Strings(out)

	return out
}

// saveNumscript saves a version of a pool name: usually the next one past the
// latest, else an out-of-order or an existing one (VERSION_ALREADY_EXISTS), and
// rarely a malformed version, an empty or garbled content, or a bad name.
func saveNumscript(ledger string, ls oracle.LedgerState) *servicepb.Request {
	name := random.RandomChoice(numscriptNames)
	if oneIn(32) {
		name = random.RandomChoice([]string{"", "bad\x01name", strings.Repeat("n", 300)})
	}

	gen, _ := generateScript()
	content := gen.Script
	switch {
	case oneIn(24):
		content = ""
	case oneIn(12):
		script := &commonpb.Script{Plain: content}
		mutateScript(script)
		content = script.Plain
	}

	return &servicepb.Request{Type: &servicepb.Request_SaveNumscript{SaveNumscript: &servicepb.SaveNumscriptRequest{
		Ledger:  ledger,
		Name:    name,
		Version: saveVersion(ls, name),
		Content: content,
	}}}
}

func saveVersion(ls oracle.LedgerState, name string) string {
	versions := storedVersions(ls, name)

	switch random.RandomChoice(indexPool(12)) {
	case 0:
		return random.RandomChoice([]string{"1.0", "v1.0.0", "", "latest", "1.0.0-rc.1"})
	case 1, 2:
		if len(versions) > 0 {
			return random.RandomChoice(versions)
		}
	case 3, 4:
		return fmt.Sprintf("%d.%d.%d", internal.Rand().Uint64()%3, internal.Rand().Uint64()%4, internal.Rand().Uint64()%4)
	}

	latest, err := semver.Parse(ls.NumscriptLatest(name))
	if err != nil {
		return "1.0.0"
	}

	switch random.RandomChoice(indexPool(3)) {
	case 0:
		return fmt.Sprintf("%d.0.0", latest.Major+1)
	case 1:
		return fmt.Sprintf("%d.%d.0", latest.Major, latest.Minor+1)
	default:
		return fmt.Sprintf("%d.%d.%d", latest.Major, latest.Minor, latest.Patch+1)
	}
}

// malformedScriptCreate is a create whose content sources collide or are
// missing: a script with postings, a script with a reference, or a nameless
// reference with or without postings.
func malformedScriptCreate(ledger string) *servicepb.Request {
	gen, _ := generateScript()
	posting := commonpb.NewPosting("world", poolAddress(), "USD/2", big.NewInt(1))

	payload := &servicepb.CreateTransactionPayload{Reference: txRef()}
	switch random.RandomChoice(indexPool(4)) {
	case 0:
		payload.Script = &commonpb.Script{Plain: gen.Script, Vars: gen.Vars}
		payload.Postings = []*commonpb.Posting{posting}
	case 1:
		payload.Script = &commonpb.Script{Plain: gen.Script, Vars: gen.Vars}
		payload.ScriptReference = &servicepb.ScriptReference{Name: random.RandomChoice(numscriptNames), Version: "latest"}
	case 2:
		payload.ScriptReference = &servicepb.ScriptReference{}
	default:
		payload.ScriptReference = &servicepb.ScriptReference{}
		payload.Postings = []*commonpb.Posting{posting}
	}

	return applyCreate(ledger, payload)
}

// validateSavedNumscriptLog holds a committed save's top-level log to the
// library entry the model stored; the server stamps CreatedAt.
func validateSavedNumscriptLog(expected *commonpb.NumscriptInfo, payload *commonpb.LogPayload) error {
	if expected == nil {
		return nil
	}

	got := payload.GetSavedNumscript().GetInfo()
	if got == nil || got.GetCreatedAt() == nil {
		return errors.New("saved numscript log missing or undated")
	}

	if got.GetName() != expected.GetName() || got.GetVersion() != expected.GetVersion() ||
		got.GetContent() != expected.GetContent() || got.GetLedger() != expected.GetLedger() {
		return fmt.Errorf("saved numscript log %s@%s on %s does not match the save of %s@%s on %s",
			got.GetName(), got.GetVersion(), got.GetLedger(), expected.GetName(), expected.GetVersion(), expected.GetLedger())
	}

	return nil
}

// scriptText is the script a create runs in g, for diagnostics.
func scriptText(g oracle.GlobalState, req *servicepb.Request) string {
	ct := req.GetApply().GetAction().GetCreateTransaction()
	if ref := ct.GetScriptReference(); ref != nil {
		ls := g.Ledger(req.GetApply().GetLedger())
		version := ref.GetVersion()
		if version == "latest" {
			version = ls.NumscriptLatest(ref.GetName())
		}

		content, _ := ls.NumscriptContent(ref.GetName(), version)

		return content
	}

	return ct.GetScript().GetPlain()
}

func scriptVars(ct *servicepb.CreateTransactionPayload) map[string]string {
	if ref := ct.GetScriptReference(); ref != nil {
		return ref.GetVars()
	}

	return ct.GetScript().GetVars()
}

// noteScriptCommit is the coverage of a validated scripted create.
func noteScriptCommit(bulk oracle.Bulk, i int, ct *servicepb.CreateTransactionPayload, order oracle.OrderResult) {
	if !oracle.IsScripted(ct) {
		return
	}

	if ref := ct.GetScriptReference(); ref != nil {
		assert.Reachable("singleton_driver_model: library script committed", internal.Details{"version": ref.GetVersion()})

		savedEarlier := slices.ContainsFunc(bulk.Requests[:i], func(req *servicepb.Request) bool {
			return req.GetSaveNumscript().GetName() == ref.GetName()
		})
		if savedEarlier {
			assert.Reachable("singleton_driver_model: script saved and run in one bulk", internal.Details{})
		}
	} else {
		assert.Reachable("singleton_driver_model: inline script committed", internal.Details{"force": ct.GetForce()})
	}

	if len(order.AccountMetadata) > len(ct.GetAccountMetadata()) {
		assert.Reachable("singleton_driver_model: script set account metadata", internal.Details{})
	}
}
