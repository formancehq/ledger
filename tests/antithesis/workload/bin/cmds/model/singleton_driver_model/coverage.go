package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Coverage probes prove the query oracle actually predicted a served response
// for each index it models, rather than merely compiling code that could have.
// A run in which an index never served a page the oracle checked says nothing
// about that index, so each probe is a must-hit `Sometimes`: satisfied the
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
)

// coveragePrefix marks every probe below. run_model_test.sh keys its
// "registered but never satisfied" gate on it, and TestCoverageMessages pins
// that no probe escapes the prefix.
const coveragePrefix = "singleton_driver_model: coverage "

// coverageIndexes is the static index set the probes are keyed on, resolved
// once: noteQueryCoverage runs on every query.
var coverageIndexes = workloadIndexes()

// coverageIndexMessage names the per-index probe. The canonical IndexID is the
// oracle's own index-map key, so the probe and the lifecycle it proves are
// keyed identically.
func coverageIndexMessage(canonical string) string {
	return coveragePrefix + "index " + canonical + " served a model-verified page"
}

// coverageMetadataMessage names the per-target metadata-index probe. Metadata
// indexes are one per declared field, a set that churns as the schema changes,
// so they are covered by shape rather than by identity: an unbounded, unstable
// family of names cannot be registered up front.
func coverageMetadataMessage(target commonpb.QueryTarget) string {
	return coveragePrefix + "a metadata-field index served a model-verified page on " + coverageTargetName(target)
}

// coverageTargetName names one of the two entity targets; metadata probes exist
// for no others.
func coverageTargetName(target commonpb.QueryTarget) string {
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		return "transactions"
	}

	return "accounts"
}

// coverageRetypeMessage names the probe for the retype exception: a page served
// while one of its indexes still had an open retype window. That window is the
// subtlest state the oracle models — the served version may still be bound to a
// superseded declared type — and nothing else proves the exception was
// exercised rather than merely compiled.
const coverageRetypeMessage = coveragePrefix + "a query was served while a retype window was open"

const (
	coverageDeletionMessage    = coveragePrefix + "ledger deletion and reserved-name rejection verified"
	coveragePromotionMessage   = coveragePrefix + "mirror promotion and write recovery verified"
	coverageMaintenanceMessage = coveragePrefix + "concurrent maintenance rejection verified"
)

// Query probes are evaluated by served pages; lifecycle probes are evaluated
// when their generated concurrent outcomes pass model validation.
func queryCoverageMessages() []string {
	out := make([]string, 0, len(coverageIndexes)+3)
	for _, wi := range coverageIndexes {
		out = append(out, coverageIndexMessage(wi.canonical))
	}

	out = append(out, applyCoverageMessages()...)
	return append(out,
		coverageMetadataMessage(commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS),
		coverageMetadataMessage(commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS),
		coverageRetypeMessage,
	)
}

func checkpointCoverageMessages() []string {
	return []string{checkpointCreateCoverage, checkpointDeleteCoverage, checkpointReadCoverage,
		checkpointDeletedReadCoverage, checkpointListCoverage, checkpointLimitCoverage,
		checkpointScheduleSetCoverage, checkpointScheduleDeleteCoverage, checkpointScheduleReadCoverage}
}

func coverageMessages() []string {
	out := append(queryCoverageMessages(), checkpointCoverageMessages()...)
	return append(out, coverageDeletionMessage, coveragePromotionMessage, coverageMaintenanceMessage)
}

// registerCoverage declares every probe before the run loop starts, so one that
// is never evaluated is still visible as an unsatisfied property instead of
// being absent from the output altogether.
func registerCoverage() {
	for _, msg := range coverageMessages() {
		assertCoverage(false, msg, nil, false)
	}
}

// emitCoverage records one evaluation of a sonde: the code point was reached,
// and cond says whether this outcome satisfies it.
func emitCoverage(cond bool, msg string, details internal.Details) {
	recordCoverageHit(msg, cond)

	assertCoverage(cond, msg, details, true)
}

// assertCoverage emits one probe observation. reached distinguishes a
// registration, which declares the probe without evaluating it, from an
// evaluation; a registration's cond carries no meaning.
func assertCoverage(cond bool, msg string, details internal.Details, reached bool) {
	// The two zeros are line and column: these probes are registered by name,
	// not discovered at a source location.
	assert.AssertRaw(cond, msg, details, coverageClass, "noteQueryCoverage", coverageFile, 0, 0,
		reached, true, "sometimes", "Sometimes", msg)
}

// noteQueryCoverage records what one validated query outcome proves. Every
// probe is evaluated on every call, not only the satisfied one — the false
// evaluations are what let Antithesis bias toward a starved index. The caller
// must not hold c.mu.
func (c *Checker) noteQueryCoverage(ledger string, target commonpb.QueryTarget, filter *commonpb.QueryFilter, needed map[string]struct{}, verified bool, rows int) {
	details := internal.Details{"ledger": ledger}

	for msg, cond := range coverageHits(target, filter, needed, verified, rows,
		c.retypeWindowOpenFor(ledger, needed)) {
		emitCoverage(cond, msg, details)
	}
}

// coverageHits is the per-probe verdict for one query outcome: message to
// whether this outcome satisfies it.
//
// A probe needs three things at once. The oracle must have ACCEPTED the
// outcome; the server must have returned at least one ROW, because a verified
// empty page proves the index was consulted but never that it can produce a
// matching record — and the generator emits deliberately unmatchable filters,
// so empty pages are the common case; and the filter must have NEEDED that
// index, so a page served through one index cannot vouch for another.
func coverageHits(target commonpb.QueryTarget, filter *commonpb.QueryFilter, needed map[string]struct{}, verified bool, rows int, retypeOpen bool) map[string]bool {
	served := verified && rows > 0
	out := make(map[string]bool, len(coverageIndexes)+2)

	for _, wi := range coverageIndexes {
		_, want := needed[wi.canonical]
		out[coverageIndexMessage(wi.canonical)] = served && want
	}

	// Only the entity targets have metadata fields; a LOGS filter cannot need a
	// metadata index, so folding it in would report against an entity probe it
	// can never satisfy.
	if target != commonpb.QueryTarget_QUERY_TARGET_LOGS {
		out[coverageMetadataMessage(target)] = served && filterNeedsMetadataIndex(filter)
	}

	out[coverageRetypeMessage] = served && retypeOpen

	return out
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

const (
	checkpointCreateCoverage         = coveragePrefix + "checkpoint creation matched the oracle"
	checkpointDeleteCoverage         = coveragePrefix + "checkpoint deletion matched the oracle"
	checkpointReadCoverage           = coveragePrefix + "checkpoint served a frozen model-verified read"
	checkpointDeletedReadCoverage    = coveragePrefix + "deleted checkpoint read returned typed NOT_FOUND"
	checkpointListCoverage           = coveragePrefix + "checkpoint listing matched the oracle"
	checkpointLimitCoverage          = coveragePrefix + "checkpoint limit returned typed CHECKPOINT_LIMIT_REACHED"
	checkpointScheduleSetCoverage    = coveragePrefix + "checkpoint schedule set matched the oracle"
	checkpointScheduleDeleteCoverage = coveragePrefix + "checkpoint schedule deletion matched the oracle"
	checkpointScheduleReadCoverage   = coveragePrefix + "checkpoint schedule read matched the oracle"
)

func noteCheckpointCoverage(message string) {
	for _, msg := range checkpointCoverageMessages() {
		emitCoverage(msg == message, msg, nil)
	}
}
