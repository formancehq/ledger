package query

import (
	"fmt"
	"math"
	"slices"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// AuditIndexReader is the read surface the audit filter compiler needs from the
// readstore secondary index (EN-1339). *readstore.Store satisfies it; tests can
// substitute a fake.
type AuditIndexReader interface {
	AuditSeqsByString(field byte, value string) ([]uint64, error)
	AuditSeqsByOutcome(success bool) ([]uint64, error)
	AuditSeqsByUint64Range(field byte, lo, hi uint64) ([]uint64, error)
}

// auditSeqUniverse is the sentinel returned by leaf compilation when a
// condition does not narrow the candidate set to an index-backed sequence
// list — specifically AUDIT_FIELD_SEQUENCE, which is served by bounding the
// audit-zone scan rather than by an index lookup. It is carried separately so
// the compiler can combine it with the zone bounds instead of the index sets.
//
// A nil []uint64 means "no matches"; to distinguish "matches everything, not
// index-narrowed" we use this typed marker returned alongside the set.
type auditCompiled struct {
	// seqs, when narrowed is true, is the ascending, de-duplicated set of audit
	// sequences that satisfy the (indexable part of the) filter.
	seqs []uint64
	// narrowed reports whether seqs constrains the result. When false the
	// filter placed no index-backed constraint (only a sequence-range bound,
	// captured separately in loSeq/hiSeq) and every entry in the zone range is
	// a candidate.
	narrowed bool
	// loSeq/hiSeq bound the audit-zone scan (inclusive) from AUDIT_FIELD_SEQUENCE
	// conditions. Defaults span the whole zone.
	loSeq uint64
	hiSeq uint64
}

// CompileAuditFilter turns a QueryFilter into the set of audit sequences that
// satisfy it, resolved entirely through the audit secondary index plus an
// audit-zone sequence bound. It returns:
//   - seqs: ascending candidate sequences when the filter is index-narrowed
//     (narrowed=true); otherwise nil.
//   - loSeq, hiSeq: inclusive audit-sequence bounds to apply to the zone scan.
//   - narrowed: whether seqs constrains the result.
//
// Only AUDIT_FIELD_* conditions and And/Or over them are accepted. Every other
// QueryFilter variant — NOT, metadata/address/ledger/log conditions, a
// non-indexed audit field — is rejected with InvalidArgument. There is no
// scan-time predicate fallback: the audit trail is queried exclusively through
// its access paths, so an expression the index cannot answer is refused rather
// than silently degrading to a full-chain scan (EN-1241 / invariant: audit is
// the source of truth, projections are the only query surface).
func CompileAuditFilter(idx AuditIndexReader, filter *commonpb.QueryFilter) (seqs []uint64, loSeq, hiSeq uint64, narrowed bool, err error) {
	if filter == nil {
		return nil, 0, math.MaxUint64, false, nil
	}

	c, err := compileAuditNode(idx, filter, 0)
	if err != nil {
		return nil, 0, 0, false, err
	}

	return c.seqs, c.loSeq, c.hiSeq, c.narrowed, nil
}

// compileAuditNode recursively compiles a filter node. depth bounds the
// and/or nesting so a maliciously (or accidentally) deep proto tree returns
// InvalidArgument instead of overflowing the Go stack — mirroring the shared
// query.Compile depth guard (MaxFilterDepth).
func compileAuditNode(idx AuditIndexReader, filter *commonpb.QueryFilter, depth int) (auditCompiled, error) {
	if depth >= MaxFilterDepth {
		return auditCompiled{}, status.Errorf(codes.InvalidArgument,
			"audit filter exceeds maximum nesting depth (%d)", MaxFilterDepth)
	}

	// Gate on the single source of truth (the generated per-target validity
	// table): only condition kinds declared valid on QUERY_TARGET_AUDIT reach a
	// compiler. This is the same table query.Compile consults for the other
	// targets, so audit-condition validity is declared alongside them rather
	// than as an undocumented exception. Concretely it admits audit[...] leaves
	// and and/or, and rejects not and every non-audit condition — matching the
	// dispatch below.
	kind := commonpb.ConditionKindOf(filter)
	if !commonpb.ConditionValidForTarget(commonpb.QueryTarget_QUERY_TARGET_AUDIT, kind) {
		return auditCompiled{}, status.Errorf(codes.InvalidArgument,
			"unsupported filter for audit entries: only audit[...] conditions combined with and/or are allowed")
	}

	switch f := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_Audit:
		return compileAuditLeaf(idx, f.Audit)
	case *commonpb.QueryFilter_And:
		return compileAuditAnd(idx, f.And.GetFilters(), depth+1)
	case *commonpb.QueryFilter_Or:
		return compileAuditOr(idx, f.Or.GetFilters(), depth+1)
	default:
		// Unreachable: the table gate above admits only Audit/And/Or on the
		// audit target. Kept as a defensive loud failure (invariant #7) in case
		// the table and this dispatch ever diverge.
		return auditCompiled{}, status.Errorf(codes.InvalidArgument,
			"unsupported filter for audit entries: only audit[...] conditions combined with and/or are allowed")
	}
}

// unconstrained is a leaf/branch that imposes no constraint at all (spans the
// whole zone, not index-narrowed).
func unconstrained() auditCompiled {
	return auditCompiled{loSeq: 0, hiSeq: math.MaxUint64, narrowed: false}
}

func compileAuditLeaf(idx AuditIndexReader, cond *commonpb.AuditCondition) (auditCompiled, error) {
	switch cond.GetField() {
	case commonpb.AuditField_AUDIT_FIELD_SEQUENCE:
		return compileAuditSeqBound(cond)
	case commonpb.AuditField_AUDIT_FIELD_OUTCOME:
		return compileAuditOutcome(idx, cond)
	case commonpb.AuditField_AUDIT_FIELD_LEDGER:
		return indexStringLeaf(idx, readstore.AuditFieldLedger, cond)
	case commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT:
		return indexStringLeaf(idx, readstore.AuditFieldCallerSubject, cond)
	case commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE:
		return indexStringLeaf(idx, readstore.AuditFieldOrderType, cond)
	case commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID:
		return indexUintLeaf(idx, readstore.AuditFieldProposalID, cond)
	case commonpb.AuditField_AUDIT_FIELD_TIMESTAMP:
		return indexUintLeaf(idx, readstore.AuditFieldTimestamp, cond)
	case commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE:
		return indexUintLeaf(idx, readstore.AuditFieldLogSeq, cond)
	default:
		return auditCompiled{}, status.Errorf(codes.InvalidArgument,
			"unsupported audit field %s", cond.GetField())
	}
}

// compileAuditSeqBound turns an AUDIT_FIELD_SEQUENCE range into inclusive
// audit-zone scan bounds rather than an index lookup — the sequence is the
// zone key itself.
func compileAuditSeqBound(cond *commonpb.AuditCondition) (auditCompiled, error) {
	uc := cond.GetUintCond()
	if uc == nil {
		return auditCompiled{}, status.Error(codes.InvalidArgument,
			"audit[seq] requires a numeric condition")
	}

	bounds, err := resolveUintBounds(uc, nil)
	if err != nil {
		return auditCompiled{}, status.Errorf(codes.InvalidArgument, "audit[seq]: %v", err)
	}

	out := unconstrained()
	if bounds.empty {
		// Impossible range (e.g. seq > MaxUint64): match nothing.
		return auditCompiled{seqs: nil, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
	}
	if bounds.hasMin {
		out.loSeq = bounds.min
	}
	if bounds.hasMax {
		// bounds are half-open [min, max); convert to inclusive hi.
		if bounds.max == 0 {
			return auditCompiled{seqs: nil, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
		}
		out.hiSeq = bounds.max - 1
	}
	if out.loSeq > out.hiSeq {
		return auditCompiled{seqs: nil, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
	}

	return out, nil
}

func compileAuditOutcome(idx AuditIndexReader, cond *commonpb.AuditCondition) (auditCompiled, error) {
	sc := cond.GetStringCond()
	if sc == nil {
		return auditCompiled{}, status.Error(codes.InvalidArgument,
			"audit[outcome] requires a string condition")
	}

	val := sc.GetHardcoded()
	var success bool
	switch val {
	case "success":
		success = true
	case "failure":
		success = false
	default:
		return auditCompiled{}, status.Errorf(codes.InvalidArgument,
			"audit[outcome] must be \"success\" or \"failure\", got %q", val)
	}

	seqs, err := idx.AuditSeqsByOutcome(success)
	if err != nil {
		return auditCompiled{}, fmt.Errorf("audit outcome index lookup: %w", err)
	}

	return auditCompiled{seqs: seqs, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
}

func indexStringLeaf(idx AuditIndexReader, field byte, cond *commonpb.AuditCondition) (auditCompiled, error) {
	sc := cond.GetStringCond()
	if sc == nil {
		return auditCompiled{}, status.Errorf(codes.InvalidArgument,
			"audit field %s requires a string condition", cond.GetField())
	}
	if sc.GetParam() != "" {
		// Audit filters are resolved without a parameter-binding context.
		return auditCompiled{}, status.Errorf(codes.InvalidArgument,
			"audit field %s does not support parameters", cond.GetField())
	}

	seqs, err := idx.AuditSeqsByString(field, sc.GetHardcoded())
	if err != nil {
		return auditCompiled{}, fmt.Errorf("audit string index lookup: %w", err)
	}

	return auditCompiled{seqs: seqs, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
}

func indexUintLeaf(idx AuditIndexReader, field byte, cond *commonpb.AuditCondition) (auditCompiled, error) {
	uc := cond.GetUintCond()
	if uc == nil {
		return auditCompiled{}, status.Errorf(codes.InvalidArgument,
			"audit field %s requires a numeric condition", cond.GetField())
	}

	bounds, err := resolveUintBounds(uc, nil)
	if err != nil {
		return auditCompiled{}, status.Errorf(codes.InvalidArgument, "audit field %s: %v", cond.GetField(), err)
	}
	if bounds.empty {
		return auditCompiled{seqs: nil, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
	}

	lo := uint64(0)
	if bounds.hasMin {
		lo = bounds.min
	}
	hi := uint64(math.MaxUint64)
	if bounds.hasMax {
		if bounds.max == 0 {
			return auditCompiled{seqs: nil, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
		}
		hi = bounds.max - 1
	}
	if lo > hi {
		return auditCompiled{seqs: nil, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
	}

	seqs, err := idx.AuditSeqsByUint64Range(field, lo, hi)
	if err != nil {
		return auditCompiled{}, fmt.Errorf("audit uint index lookup: %w", err)
	}

	return auditCompiled{seqs: seqs, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
}

func compileAuditAnd(idx AuditIndexReader, filters []*commonpb.QueryFilter, depth int) (auditCompiled, error) {
	if len(filters) == 0 {
		// An empty AND conventionally matches everything; but for audit we treat
		// a degenerate empty filter as unconstrained rather than error.
		return unconstrained(), nil
	}

	acc := unconstrained()
	for _, f := range filters {
		c, err := compileAuditNode(idx, f, depth)
		if err != nil {
			return auditCompiled{}, err
		}

		acc = intersectAudit(acc, c)
	}

	return acc, nil
}

func compileAuditOr(idx AuditIndexReader, filters []*commonpb.QueryFilter, depth int) (auditCompiled, error) {
	if len(filters) == 0 {
		return auditCompiled{seqs: nil, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}, nil
	}

	var acc auditCompiled
	first := true
	for _, f := range filters {
		c, err := compileAuditNode(idx, f, depth)
		if err != nil {
			return auditCompiled{}, err
		}

		// OR requires every disjunct to be index-narrowed: a non-narrowed
		// disjunct (a bare seq-range) would union to "everything in that
		// range", which the seq-set representation cannot express alongside an
		// index set. Reject rather than over-match.
		if !c.narrowed {
			return auditCompiled{}, status.Error(codes.InvalidArgument,
				"audit[seq] cannot appear inside an or; combine sequence bounds with and, or filter on an indexed field")
		}

		if first {
			acc = c
			first = false

			continue
		}

		acc = auditCompiled{seqs: unionSorted(acc.seqs, c.seqs), narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}
	}

	return acc, nil
}

// intersectAudit combines two compiled sub-filters under AND semantics,
// reconciling the index-narrowed seq sets and the zone sequence bounds.
//
// Invariant maintained for the caller: a NARROWED result returned here has its
// [loSeq, hiSeq] window already baked into seqs (and its bounds reset to full).
// A branch that mixes an index seq-set with a residual seq bound (e.g.
// `outcome == failure and seq < 10`) would otherwise carry that bound only on
// hiSeq; an enclosing OR unions seqs but cannot represent per-branch bounds, so
// it would leak entries outside the branch predicate. Baking the bound into the
// branch's seqs before it can be unioned closes that gap (NumaryBot finding).
func intersectAudit(a, b auditCompiled) auditCompiled {
	lo := max(a.loSeq, b.loSeq)
	hi := min(a.hiSeq, b.hiSeq)

	switch {
	case a.narrowed && b.narrowed:
		return bakeBounds(auditCompiled{seqs: intersectSorted(a.seqs, b.seqs), narrowed: true, loSeq: lo, hiSeq: hi})
	case a.narrowed:
		return bakeBounds(auditCompiled{seqs: a.seqs, narrowed: true, loSeq: lo, hiSeq: hi})
	case b.narrowed:
		return bakeBounds(auditCompiled{seqs: b.seqs, narrowed: true, loSeq: lo, hiSeq: hi})
	default:
		// Neither side is index-narrowed: only seq bounds constrain the result,
		// which the caller carries forward as a zone-scan window.
		return auditCompiled{narrowed: false, loSeq: lo, hiSeq: hi}
	}
}

// bakeBounds materializes a narrowed result's [loSeq, hiSeq] window into its seq
// set and resets the window to full, so the bound cannot be lost when the result
// is later unioned by an enclosing OR. No-op for a non-narrowed result or one
// whose window is already unconstrained.
func bakeBounds(c auditCompiled) auditCompiled {
	if !c.narrowed || (c.loSeq == 0 && c.hiSeq == math.MaxUint64) {
		return c
	}

	filtered := c.seqs[:0:0]
	for _, s := range c.seqs {
		if s >= c.loSeq && s <= c.hiSeq {
			filtered = append(filtered, s)
		}
	}

	return auditCompiled{seqs: filtered, narrowed: true, loSeq: 0, hiSeq: math.MaxUint64}
}

// intersectSorted returns the sorted intersection of two ascending, de-duped
// sequence slices.
func intersectSorted(a, b []uint64) []uint64 {
	var out []uint64
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			i++
		default:
			j++
		}
	}

	return out
}

// unionSorted returns the sorted union of two ascending, de-duped sequence
// slices.
func unionSorted(a, b []uint64) []uint64 {
	out := make([]uint64, 0, len(a)+len(b))
	i, j := 0, 0
	for i < len(a) && j < len(b) {
		switch {
		case a[i] == b[j]:
			out = append(out, a[i])
			i++
			j++
		case a[i] < b[j]:
			out = append(out, a[i])
			i++
		default:
			out = append(out, b[j])
			j++
		}
	}
	out = append(out, a[i:]...)
	out = append(out, b[j:]...)

	return slices.Clip(out)
}
