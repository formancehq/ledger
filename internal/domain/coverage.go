package domain

import "errors"

// CoverageContractViolation returns the admission-contract violation carried by
// err — a coverage miss (ErrReasonCoverageMiss) or a structurally inconsistent
// execution plan (ErrReasonInvalidExecutionPlan) — or nil when err is neither.
//
// Both mean the FSM apply path read a key the proposer never declared, or was
// handed a plan that cannot be applied: "should not happen" bugs (invariant #7)
// that must reach the audit chain, the frozen idempotency outcome and the client
// under their own reason, never relabelled as a storage fault.
//
// It matches on the stable domain Reason string rather than the concrete type so
// callers need not import internal/infra/state, which owns *ErrCoverageMiss and
// itself imports internal/domain/processing (an import cycle in the other
// direction). Every Describable in the chain is inspected because intermediate
// wrappers — ErrStorageOperation, or the numscript library's QueryBalanceError /
// QueryMetadataError — implement Unwrap.
//
// Limitation: the walk follows single-error Unwrap only. An error joined with
// errors.Join hides its members from this check.
func CoverageContractViolation(err error) Describable {
	for e := err; e != nil; e = errors.Unwrap(e) {
		describable, ok := e.(Describable) //nolint:errorlint // deliberate per-node check; the loop unwraps the chain itself
		if !ok {
			continue
		}

		switch describable.Reason() {
		case ErrReasonCoverageMiss, ErrReasonInvalidExecutionPlan:
			return describable
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
func StoreFailure(operation string, err error) Describable {
	if violation := CoverageContractViolation(err); violation != nil {
		return violation
	}

	return &ErrStorageOperation{Operation: operation, Cause: err}
}
