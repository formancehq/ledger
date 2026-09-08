package domain

import "errors"

// CoverageContractViolation returns the admission-contract violation carried by
// err — a coverage miss (ErrReasonCoverageMiss) or a structurally inconsistent
// execution plan (ErrReasonInvalidExecutionPlan) — or nil when err is neither.
//
// Both mean the FSM apply path read a key the proposer never declared, or was
// handed a plan that cannot be applied: "should not happen" bugs (invariant #7)
// that must reach the audit chain and the client under their own reason, never
// relabelled as a storage fault. They never reach a frozen idempotency outcome:
// KindForReason maps both reasons to KindInternal, which IsFreezableFailure
// excludes, so recordIdempotencyFailure returns early — see
// docs/technical/architecture/subsystems/fsm/coverage-gate.md.
//
// It matches on the stable domain Reason string rather than the concrete type so
// callers need not import internal/infra/state, which owns *ErrCoverageMiss and
// itself imports internal/domain/processing (an import cycle in the other
// direction). Every Describable in the chain is inspected because intermediate
// wrappers — ErrStorageOperation, or the numscript library's QueryBalanceError /
// QueryMetadataError — implement Unwrap.
//
// The walk also descends into multi-error nodes (errors.Join, or fmt.Errorf with
// several %w verbs), which errors.Unwrap cannot follow. Members are visited in
// slice order, so the Describable returned for a given tree is deterministic —
// a requirement on the FSM apply path (invariant #2). Without this the forbidigo
// rule guarding StoreFailure would not help: a future Join is a plain call the
// linter cannot see, and it would silently relabel the violation as a storage
// fault in the immutable audit chain.
func CoverageContractViolation(err error) Describable {
	for e := err; e != nil; e = errors.Unwrap(e) {
		if describable, ok := e.(Describable); ok { //nolint:errorlint // deliberate per-node check; the loop walks the chain itself
			switch describable.Reason() {
			case ErrReasonCoverageMiss, ErrReasonInvalidExecutionPlan:
				return describable
			}
		}

		// A multi-error node reports no single cause, so errors.Unwrap returns
		// nil and this iteration is the last: recurse before the loop ends.
		joined, ok := e.(interface{ Unwrap() []error })
		if !ok {
			continue
		}

		for _, member := range joined.Unwrap() {
			if violation := CoverageContractViolation(member); violation != nil {
				return violation
			}
		}
	}

	return nil
}

// StoreFailure returns the Describable for a failed store operation.
//
// An admission-contract violation is propagated verbatim so its reason and
// metadata survive to the audit chain; anything else is wrapped as
// ErrStorageOperation under the given operation label. Every FSM read site must
// build its failure through this function rather than constructing an
// ErrStorageOperation directly — a bare construction swallows the coverage
// reason, which is enforced by a forbidigo rule in .golangci.yaml.
//
// operation is the short identifier ErrStorageOperation surfaces ("loading
// ledger", "checking transaction reference"). err is expected to be non-nil;
// calling with nil yields an ErrStorageOperation with a nil cause.
// ErrSequenceSpaceExhausted is returned when a monotone FSM counter has no
// successor left: the next allocation would be math.MaxUint64 and the counter
// would then wrap to 0, handing out sequences that already address existing
// rows.
//
// Unreachable by allocation — the counters are seeded at 1 and advanced by one,
// so the top of the space is 2^64 proposals away. It is reachable by restoring
// a tampered export: backup key validation checks the key prefix, the key
// length and a range taken from the manifest itself, so a segment declaring a
// head near the top passes. Recovery refuses to boot on a head of MaxUint64
// (state.LoadFSMStateFromStore), but a head just below it boots and wraps on
// the next allocation, which is why the guard also lives at the allocator.
//
// Deterministic: the counter is replicated state, so every node reaches this
// on the same entry and fails the same proposal (invariant #2).
var ErrSequenceSpaceExhausted = errors.New("FSM sequence space exhausted")

func StoreFailure(operation string, err error) Describable {
	if violation := CoverageContractViolation(err); violation != nil {
		return violation
	}

	return &ErrStorageOperation{Operation: operation, Cause: err}
}
