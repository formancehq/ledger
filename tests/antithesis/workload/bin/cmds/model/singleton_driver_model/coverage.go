package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Coverage sondes prove the query oracle actually predicted a served response
// for each index it models, rather than merely compiling code that could have.
// A run in which an index never served a page the oracle checked says nothing
// about that index, so each sonde is a must-hit `Sometimes`: satisfied the
// first time that index serves a verified page, and evaluated false on every
// other query so Antithesis has a gradient to steer along. `Reachable` cannot
// do this — its condition is hard-wired true, so it never produces the failing
// evaluations the platform's guidance biases on.
//
// The names are data-driven (one per index), so the antithesis-go-instrumentor
// cannot catalogue them: it only resolves literal message arguments. They are
// registered at run time through assert.AssertRaw instead — the same not-hit
// emission the generated catalog performs for literal assertions — and the
// hit-time emissions reuse the identical message/ID so they land on the
// registered property. This mirrors internal/block/block.go, and AssertRaw is
// invisible to the scanner, so no anonymous catalog entries are produced.
const (
	coverageClass = "github.com/formancehq/ledger/v3/tests/antithesis/workload/bin/cmds/model/singleton_driver_model"
	coverageFile  = "tests/antithesis/workload/bin/cmds/model/singleton_driver_model/coverage.go"

	coverageHit    = true
	coverageNotHit = false
)

// coveragePrefix marks every sonde below. run_model_test.sh keys its
// "registered but never satisfied" gate on it, and TestCoverageMessages pins
// that no sonde escapes the prefix.
const coveragePrefix = "singleton_driver_model: coverage "

// coverageIndexes is the static index set the sondes are keyed on, resolved
// once: noteQueryCoverage runs on every query.
var coverageIndexes = workloadIndexes()

// coverageIndexMessage names the per-index sonde. The canonical IndexID is the
// oracle's own index-map key, so the sonde and the lifecycle it proves are
// keyed identically.
func coverageIndexMessage(canonical string) string {
	return coveragePrefix + "index " + canonical + " served a model-verified page"
}

// coverageMetadataMessage names the per-target metadata-index sonde. Metadata
// indexes are one per declared field, a set that churns as the schema changes,
// so they are covered by shape rather than by identity: an unbounded, unstable
// family of names cannot be registered up front.
func coverageMetadataMessage(target commonpb.QueryTarget) string {
	return coveragePrefix + "a metadata-field index served a model-verified page on " + coverageTargetName(target)
}

// coverageTargetName names one of the two entity targets; metadata sondes exist
// for no others.
func coverageTargetName(target commonpb.QueryTarget) string {
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		return "transactions"
	}

	return "accounts"
}

// coverageRetypeMessage names the sonde for the retype exception: a page served
// while one of its indexes still had an open retype window. That window is the
// subtlest state the oracle models — the served version may still be bound to a
// superseded declared type — and nothing else proves the exception was
// exercised rather than merely compiled.
const coverageRetypeMessage = coveragePrefix + "a query was served while a retype window was open"

// coverageMessages is every sonde this driver registers, in registration order.
func coverageMessages() []string {
	out := make([]string, 0, len(coverageIndexes)+3)
	for _, wi := range coverageIndexes {
		out = append(out, coverageIndexMessage(wi.canonical))
	}

	return append(out,
		coverageMetadataMessage(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS),
		coverageMetadataMessage(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS),
		coverageRetypeMessage,
	)
}

// registerCoverage declares every sonde before the run loop starts, so one that
// is never evaluated is still visible as an unsatisfied property instead of
// being absent from the output altogether.
func registerCoverage() {
	for _, msg := range coverageMessages() {
		emitCoverage(false, msg, nil, coverageNotHit)
	}
}

func emitCoverage(cond bool, msg string, details internal.Details, hit bool) {
	assert.AssertRaw(cond, msg, details, coverageClass, "noteQueryCoverage", coverageFile, 0,
		hit, true, "sometimes", "Sometimes", msg)
}

// noteQueryCoverage records what one validated query outcome proves. served is
// true only when the oracle accepted the outcome AND the server returned rows:
// a refusal the model predicted shows the lifecycle gate works, not that the
// index can answer. needed is the filter's index requirement, so a sonde can
// only be satisfied by a query that genuinely depended on that index.
//
// Every sonde is evaluated on every call, not only the satisfied one — the
// false evaluations are what let Antithesis bias toward a starved index. The
// caller must not hold c.mu.
func (c *Checker) noteQueryCoverage(ledger string, target commonpb.QueryTarget, filter *commonpb.QueryFilter, needed map[string]struct{}, served bool) {
	details := internal.Details{"ledger": ledger}

	for _, wi := range coverageIndexes {
		_, want := needed[wi.canonical]
		emitCoverage(served && want, coverageIndexMessage(wi.canonical), details, coverageHit)
	}

	// Only the entity targets have metadata fields; a LOGS filter cannot need a
	// metadata index, so folding it in would report against an entity sonde it
	// can never satisfy.
	if target != commonpb.QueryTarget_QUERY_TARGET_LOGS {
		emitCoverage(served && filterNeedsMetadataIndex(filter),
			coverageMetadataMessage(target), details, coverageHit)
	}

	emitCoverage(served && c.retypeWindowOpenFor(ledger, needed),
		coverageRetypeMessage, details, coverageHit)
}

// filterNeedsMetadataIndex reports whether any leaf is a metadata-field
// condition — the leaves neededIndexCanonicals maps to a per-(target, key)
// metadata index. Read off the filter rather than sniffed out of the canonical
// strings, so it stays exact as the IndexID encoding changes.
func filterNeedsMetadataIndex(f *commonpb.QueryFilter) bool {
	found := false

	visitLeaves(f, func(leaf *commonpb.QueryFilter) {
		if _, ok := leaf.GetFilter().(*commonpb.QueryFilter_Field); ok {
			found = true
		}
	})

	return found
}

// retypeWindowOpenFor reports whether any of the query's indexes still had an
// open retype window on the committed state. The window is read there rather
// than from the candidate base the match came from, which matchesModel does not
// report; the committed state is the same source the finding diagnostics use.
func (c *Checker) retypeWindowOpenFor(ledger string, needed map[string]struct{}) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	ls := c.modelState.Ledger(ledger)
	for canon := range needed {
		if _, open := ls.RetypeWindow(canon); open {
			return true
		}
	}

	return false
}
