package query

import (
	"slices"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// AuditPredicate decides whether an audit entry matches a compiled filter.
// items is non-nil only when the filter references AUDIT_FIELD_ORDER_TYPE
// (see CompileAuditPredicate's needsItems return); other fields ignore it.
type AuditPredicate func(entry *auditpb.AuditEntry, items []*auditpb.AuditItem) bool

// CompileAuditPredicate turns a QueryFilter into a scan-time predicate over
// audit entries. needsItems is true iff the filter references ORDER_TYPE, so the
// caller only pays the per-entry AuditItem load when required. Unsupported
// QueryFilter variants and field/condition mismatches return a
// FilterCompilationError (mapped to gRPC InvalidArgument).
func CompileAuditPredicate(filter *commonpb.QueryFilter) (AuditPredicate, bool, error) {
	if filter == nil {
		return func(*auditpb.AuditEntry, []*auditpb.AuditItem) bool { return true }, false, nil
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
		return func(e *auditpb.AuditEntry, items []*auditpb.AuditItem) bool { return !inner(e, items) }, nil
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
	return func(e *auditpb.AuditEntry, items []*auditpb.AuditItem) bool {
		for _, p := range preds {
			if !p(e, items) {
				return false
			}
		}
		return true
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
	return func(e *auditpb.AuditEntry, items []*auditpb.AuditItem) bool {
		for _, p := range preds {
			if p(e, items) {
				return true
			}
		}
		return false
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
			return []string{e.GetFailure().GetErrorType()}
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
	return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) bool {
		return matchUintBounds(bounds, get(e))
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
	return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) bool {
		s := e.GetSuccess()
		if s == nil {
			return false
		}
		if bounds.empty {
			return false
		}
		lo, hi := s.GetMinLogSequence(), s.GetMaxLogSequence()
		// Overlap of [lo,hi] with half-open [min,max).
		if bounds.hasMin && hi < bounds.min {
			return false
		}
		if bounds.hasMax && lo >= bounds.max {
			return false
		}
		return true
	}, nil
}

// outcomePredicate matches a string condition whose value is "success" or
// "failure".
func outcomePredicate(cond *commonpb.AuditCondition) (AuditPredicate, error) {
	sc := cond.GetStringCond()
	if sc == nil {
		return nil, domain.NewFilterCompilationError("audit field outcome requires a string condition")
	}
	want := sc.GetHardcoded()
	switch want {
	case "success":
		return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) bool { return e.GetSuccess() != nil }, nil
	case "failure":
		return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) bool { return e.GetFailure() != nil }, nil
	default:
		return nil, domain.NewFilterCompilationError("audit field outcome must be \"success\" or \"failure\", got %q", want)
	}
}

// stringFieldPredicate matches a hardcoded StringCondition against one or more
// string values from the entry (match-any: success against ANY value).
func stringFieldPredicate(cond *commonpb.AuditCondition, get func(*auditpb.AuditEntry) []string) (AuditPredicate, error) {
	sc := cond.GetStringCond()
	if sc == nil {
		return nil, domain.NewFilterCompilationError("audit field %v requires a string condition", cond.GetField())
	}
	want := sc.GetHardcoded()
	return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) bool {
		return slices.Contains(get(e), want)
	}, nil
}

func boolFieldPredicate(cond *commonpb.AuditCondition, get func(*auditpb.AuditEntry) bool) (AuditPredicate, error) {
	bc := cond.GetBoolCond()
	if bc == nil {
		return nil, domain.NewFilterCompilationError("audit field %v requires a bool condition", cond.GetField())
	}
	want := bc.GetHardcoded()
	return func(e *auditpb.AuditEntry, _ []*auditpb.AuditItem) bool {
		return get(e) == want
	}, nil
}

// orderTypePredicate matches (match-any) when any order in the proposal has the
// requested payload variant name (e.g. "apply", "save_numscript"). Order type
// is not on the audit header, so the caller must supply the entry's AuditItems
// (see CompileAuditPredicate's needsItems return).
func orderTypePredicate(cond *commonpb.AuditCondition) (AuditPredicate, error) {
	sc := cond.GetStringCond()
	if sc == nil {
		return nil, domain.NewFilterCompilationError("audit field order_type requires a string condition")
	}
	want := sc.GetHardcoded()
	return func(_ *auditpb.AuditEntry, items []*auditpb.AuditItem) bool {
		for _, it := range items {
			order := &raftcmdpb.Order{}
			if err := proto.Unmarshal(it.GetSerializedOrder(), order); err != nil {
				continue // unreadable order bytes cannot match a positive filter
			}
			if orderTypeName(order) == want {
				return true
			}
		}
		return false
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
