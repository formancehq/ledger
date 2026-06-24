package query

import (
	"fmt"
	"slices"

	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// AuditPredicate decides whether an audit entry matches a compiled filter.
// items is non-nil only when the filter references AUDIT_FIELD_ORDER_TYPE
// (see CompileAuditPredicate's needsItems return); other fields ignore it.
// The error return surfaces per-entry data faults (e.g. an audit order that
// fails to unmarshal) instead of silently dropping the entry.
type AuditPredicate func(entry *auditpb.AuditEntry, items []*auditpb.AuditItem) (bool, error)

// CompileAuditPredicate turns a QueryFilter into a scan-time predicate over
// audit entries. needsItems is true iff the filter references ORDER_TYPE, so the
// caller only pays the per-entry AuditItem load when required. Unsupported
// QueryFilter variants and field/condition mismatches return a
// FilterCompilationError (mapped to gRPC InvalidArgument).
func CompileAuditPredicate(filter *commonpb.QueryFilter) (AuditPredicate, bool, error) {
	if filter == nil {
		return func(*auditpb.AuditEntry, []*auditpb.AuditItem) (bool, error) { return true, nil }, false, nil
	}

	c := &auditCompiler{}
	pred, err := c.compile(filter, 0)
	if err != nil {
		return nil, false, err
	}

	return pred, c.needsItems, nil
}

type auditCompiler struct {
	needsItems bool
}

func (c *auditCompiler) compile(filter *commonpb.QueryFilter, depth int) (AuditPredicate, error) {
	if depth >= MaxFilterDepth {
		return nil, ErrFilterTooDeep
	}

	switch f := filter.GetFilter().(type) {
	case *commonpb.QueryFilter_Audit:
		return c.compileCondition(f.Audit)
	case *commonpb.QueryFilter_And:
		return c.compileAnd(f.And, depth)
	case *commonpb.QueryFilter_Or:
		return c.compileOr(f.Or, depth)
	case *commonpb.QueryFilter_Not:
		inner, err := c.compile(f.Not.GetFilter(), depth+1)
		if err != nil {
			return nil, err
		}

		return func(e *auditpb.AuditEntry, items []*auditpb.AuditItem) (bool, error) {
			ok, err := inner(e, items)
			if err != nil {
				return false, err
			}

			return !ok, nil
		}, nil
	default:
		return nil, domain.NewFilterCompilationError("unsupported condition for audit target: %T", filter.GetFilter())
	}
}

func (c *auditCompiler) compileAnd(and *commonpb.AndFilter, depth int) (AuditPredicate, error) {
	preds := make([]AuditPredicate, 0, len(and.GetFilters()))
	for _, sub := range and.GetFilters() {
		p, err := c.compile(sub, depth+1)
		if err != nil {
			return nil, err
		}
		preds = append(preds, p)
	}

	return func(e *auditpb.AuditEntry, items []*auditpb.AuditItem) (bool, error) {
		for _, p := range preds {
			ok, err := p(e, items)
			if err != nil {
				return false, err
			}
			if !ok {
				return false, nil
			}
		}

		return true, nil
	}, nil
}

func (c *auditCompiler) compileOr(or *commonpb.OrFilter, depth int) (AuditPredicate, error) {
	preds := make([]AuditPredicate, 0, len(or.GetFilters()))
	for _, sub := range or.GetFilters() {
		p, err := c.compile(sub, depth+1)
		if err != nil {
			return nil, err
		}
		preds = append(preds, p)
	}

	return func(e *auditpb.AuditEntry, items []*auditpb.AuditItem) (bool, error) {
		for _, p := range preds {
			ok, err := p(e, items)
			if err != nil {
				return false, err
			}
			if ok {
				return true, nil
			}
		}

		return false, nil
	}, nil
}

func (c *auditCompiler) compileCondition(cond *commonpb.AuditCondition) (AuditPredicate, error) {
	switch cond.GetField() {
	case commonpb.AuditField_AUDIT_FIELD_SEQUENCE:
		return uintFieldPredicate(cond, func(e *auditpb.AuditEntry) uint64 { return e.GetSequence() })
	case commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID:
		return uintFieldPredicate(cond, func(e *auditpb.AuditEntry) uint64 { return e.GetProposalId() })
	case commonpb.AuditField_AUDIT_FIELD_TIMESTAMP:
		return uintFieldPredicate(cond, func(e *auditpb.AuditEntry) uint64 { return e.GetTimestamp().GetData() })
	case commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE:
		return logSequencePredicate(cond)
	case commonpb.AuditField_AUDIT_FIELD_OUTCOME:
		return outcomePredicate(cond)
	case commonpb.AuditField_AUDIT_FIELD_ERROR_TYPE:
		return stringFieldPredicate(cond, func(e *auditpb.AuditEntry) []string {
			if e.GetFailure() == nil {
				return nil
			}

			return []string{domain.ReasonString(e.GetFailure().GetReason())}
		})
	case commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT:
		return stringFieldPredicate(cond, func(e *auditpb.AuditEntry) []string {
			return []string{e.GetCallerSnapshot().GetIdentity().GetSubject()}
		})
	case commonpb.AuditField_AUDIT_FIELD_CALLER_SCOPE:
		return stringFieldPredicate(cond, func(e *auditpb.AuditEntry) []string {
			return e.GetCallerSnapshot().GetScopes()
		})
	case commonpb.AuditField_AUDIT_FIELD_CALLER_GOD:
		return boolFieldPredicate(cond, func(e *auditpb.AuditEntry) bool { return e.GetCallerSnapshot().GetGod() })
	case commonpb.AuditField_AUDIT_FIELD_LEDGER:
		return stringFieldPredicate(cond, func(e *auditpb.AuditEntry) []string { return e.GetLedgers() })
	case commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE:
		c.needsItems = true

		return orderTypePredicate(cond)
	default:
		return nil, domain.NewFilterCompilationError("unsupported audit field: %v", cond.GetField())
	}
}

// uintFieldPredicate evaluates a UintCondition against a single uint64 getter,
// reusing the same bounds semantics as the index compiler (half-open [min,max)).
func uintFieldPredicate(cond *commonpb.AuditCondition, get func(*auditpb.AuditEntry) uint64) (AuditPredicate, error) {
	uc := cond.GetUintCond()
	if uc == nil {
		return nil, domain.NewFilterCompilationError("audit field %v requires a uint condition", cond.GetField())
	}
	bounds, err := resolveUintBounds(uc, nil)
	if err != nil {
		return nil, err
	}

	return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) (bool, error) {
		return matchUintBounds(bounds, get(e)), nil
	}, nil
}

func matchUintBounds(b resolvedUintBounds, v uint64) bool {
	if b.empty {
		return false
	}
	if b.hasMin && v < b.min {
		return false
	}
	if b.hasMax && v >= b.max { // max is exclusive (half-open)
		return false
	}

	return true
}

// logSequencePredicate matches when the entry's produced log range
// [min,max] overlaps the requested bounds. Failures produce no logs and never
// match a positive log-sequence filter.
func logSequencePredicate(cond *commonpb.AuditCondition) (AuditPredicate, error) {
	uc := cond.GetUintCond()
	if uc == nil {
		return nil, domain.NewFilterCompilationError("audit field log_seq requires a uint condition")
	}
	bounds, err := resolveUintBounds(uc, nil)
	if err != nil {
		return nil, err
	}

	return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) (bool, error) {
		s := e.GetSuccess()
		if s == nil {
			return false, nil
		}
		if bounds.empty {
			return false, nil
		}
		lo, hi := s.GetMinLogSequence(), s.GetMaxLogSequence()
		// Overlap of [lo,hi] with half-open [min,max).
		if bounds.hasMin && hi < bounds.min {
			return false, nil
		}
		if bounds.hasMax && lo >= bounds.max {
			return false, nil
		}

		return true, nil
	}, nil
}

// hardcodedAuditString extracts the hardcoded value of cond's StringCondition.
// It rejects a missing condition and a parameterized ($param) value: audit
// filters are evaluated at scan time and have no parameter-resolution context,
// so a Param would otherwise read as GetHardcoded() == "" and silently match
// the empty string.
func hardcodedAuditString(cond *commonpb.AuditCondition, field string) (string, error) {
	sc := cond.GetStringCond()
	if sc == nil {
		return "", domain.NewFilterCompilationError("audit field %s requires a string condition", field)
	}
	if _, ok := sc.GetValue().(*commonpb.StringCondition_Param); ok {
		return "", domain.NewFilterCompilationError("audit field %s does not support parameterized conditions", field)
	}

	return sc.GetHardcoded(), nil
}

// hardcodedAuditBool extracts the hardcoded value of cond's BoolCondition.
// Like hardcodedAuditString, it rejects a missing condition and any
// non-hardcoded value (a $param, or an unset oneof): audit filters are
// evaluated at scan time with no parameter-resolution context, so a Param
// would otherwise read as GetHardcoded() == false and silently match against
// false instead of returning InvalidArgument.
func hardcodedAuditBool(cond *commonpb.AuditCondition) (bool, error) {
	bc := cond.GetBoolCond()
	if bc == nil {
		return false, domain.NewFilterCompilationError("audit field %v requires a bool condition", cond.GetField())
	}
	hc, ok := bc.GetValue().(*commonpb.BoolCondition_Hardcoded)
	if !ok {
		return false, domain.NewFilterCompilationError("audit field %v does not support parameterized conditions", cond.GetField())
	}

	return hc.Hardcoded, nil
}

// outcomePredicate matches a string condition whose value is "success" or
// "failure".
func outcomePredicate(cond *commonpb.AuditCondition) (AuditPredicate, error) {
	want, err := hardcodedAuditString(cond, "outcome")
	if err != nil {
		return nil, err
	}
	switch want {
	case "success":
		return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) (bool, error) { return e.GetSuccess() != nil, nil }, nil
	case "failure":
		return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) (bool, error) { return e.GetFailure() != nil, nil }, nil
	default:
		return nil, domain.NewFilterCompilationError("audit field outcome must be \"success\" or \"failure\", got %q", want)
	}
}

// stringFieldPredicate matches a hardcoded StringCondition against one or more
// string values from the entry (match-any: success against ANY value).
func stringFieldPredicate(cond *commonpb.AuditCondition, get func(*auditpb.AuditEntry) []string) (AuditPredicate, error) {
	want, err := hardcodedAuditString(cond, cond.GetField().String())
	if err != nil {
		return nil, err
	}

	return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) (bool, error) {
		return slices.Contains(get(e), want), nil
	}, nil
}

func boolFieldPredicate(cond *commonpb.AuditCondition, get func(*auditpb.AuditEntry) bool) (AuditPredicate, error) {
	want, err := hardcodedAuditBool(cond)
	if err != nil {
		return nil, err
	}

	return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) (bool, error) {
		return get(e) == want, nil
	}, nil
}

// orderTypePredicate matches (match-any) when any order in the proposal has the
// requested payload variant name (e.g. "apply", "save_numscript"). Order type
// is not on the audit header, so the caller must supply the entry's AuditItems
// (see CompileAuditPredicate's needsItems return).
func orderTypePredicate(cond *commonpb.AuditCondition) (AuditPredicate, error) {
	want, err := hardcodedAuditString(cond, "order_type")
	if err != nil {
		return nil, err
	}

	return func(_ *auditpb.AuditEntry, items []*auditpb.AuditItem) (bool, error) {
		for _, it := range items {
			order := &raftcmdpb.Order{}
			if err := order.UnmarshalVT(it.GetSerializedOrder()); err != nil {
				// The audit zone is the cryptographic source of truth; order
				// bytes that fail to unmarshal indicate corruption/tampering,
				// not an expected runtime condition. Surface it loudly through
				// the error-capable cursor rather than silently dropping it.
				return false, fmt.Errorf("unmarshalling audit order (index %d): %w", it.GetOrderIndex(), err)
			}
			if orderTypeName(order) == want {
				return true, nil
			}
		}

		return false, nil
	}, nil
}

// orderTypeName returns the proto field name of the active order payload oneof
// (e.g. "apply", "create_ledger", "register_signing_key"), or "" if unset.
// Derived via protoreflect so the full variant set is sourced from the proto,
// not a hand-maintained switch.
func orderTypeName(order *raftcmdpb.Order) string {
	switch t := order.GetType().(type) {
	case *raftcmdpb.Order_LedgerScoped:
		return activeOneofName(t.LedgerScoped.ProtoReflect(), "payload")
	case *raftcmdpb.Order_SystemScoped:
		return activeOneofName(t.SystemScoped.ProtoReflect(), "payload")
	default:
		return ""
	}
}

func activeOneofName(m protoreflect.Message, oneof protoreflect.Name) string {
	od := m.Descriptor().Oneofs().ByName(oneof)
	if od == nil {
		return ""
	}
	fd := m.WhichOneof(od)
	if fd == nil {
		return ""
	}

	return string(fd.Name())
}
