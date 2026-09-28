package query

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
)

// ErrAggregateOverflow signals that summing colored or precision-rescaled
// buckets in the read-side aggregator exceeded the 2^256 uint256 ceiling.
// The FSM already rejects per-bucket overflow on write (#321); this guards
// the aggregator with the same discipline since collapseColors and
// use_max_precision can sum many buckets together.
//
// This is a query-only outcome — it is produced by the read-side aggregator
// (aggregate.go), never emitted by the FSM apply path — so it lives in the
// query layer, not in internal/domain (which is reserved for FSM-generated
// business outcomes). It implements the full domain.SerializableError contract
// anyway: the reason is a shipped wire identifier clients match on, and the
// stage/side metadata is client-facing context the gRPC ErrorInfo carries. It
// reuses the domain-level wire constant domain.ErrReasonAggregateOverflow (the
// client-facing reason string and its KindForReason classification are the
// shared wire contract and stay in domain).
type ErrAggregateOverflow struct {
	Stage string // "accumulate", "collapse-colors", "max-precision-merge" or "max-precision-rescale"
	Side  string // "input" or "output"
}

func (e *ErrAggregateOverflow) Error() string {
	return fmt.Sprintf("aggregate volume %s overflowed 2^256 during %s", e.Side, e.Stage)
}
func (*ErrAggregateOverflow) Kind() domain.ErrorKind { return domain.KindPrecondition }
func (*ErrAggregateOverflow) Reason() string         { return domain.ErrReasonAggregateOverflow }
func (e *ErrAggregateOverflow) Metadata() map[string]string {
	return map[string]string{"stage": e.Stage, "side": e.Side}
}

// Compile-time assertion that ErrAggregateOverflow keeps the full contract, so
// it goes on flowing through the shared error edge (gRPC/HTTP mapping) with its
// reason and structured context intact.
var _ domain.SerializableError = (*ErrAggregateOverflow)(nil)

// The two errors below reject a malformed ExecutePreparedQuery request. They
// implement domain.Classifiable and nothing more, which is the whole point:
// the adapters need a kind to pick a status code, and neither failure needs a
// stable public identifier. A Reason is a versioned wire contract — once a
// client can pattern-match it, it can never be renamed — so a read-path
// argument check that no client branches on must not mint one. They reach the
// caller as the right status code with no ErrorInfo and no audit surface.
//
// Before EN-2081 both were bare errors.New values, which the gRPC sanitiser
// answered as codes.Unknown; classifying them turns a caller mistake into the
// 400 it always was.

// ErrPreparedQueryAggregateTarget rejects an AGGREGATE_VOLUMES execution of a
// prepared query whose target is not ACCOUNTS. Volume aggregation only has a
// meaning over accounts, and the target is fixed by the stored definition, so
// the caller must either execute it in LIST mode or aggregate another query.
type ErrPreparedQueryAggregateTarget struct {
	Target commonpb.QueryTarget
}

func (e *ErrPreparedQueryAggregateTarget) Error() string {
	return "AGGREGATE_VOLUMES mode is only valid for ACCOUNTS target queries, this query targets " + commonpb.TargetHumanName(e.Target)
}

func (*ErrPreparedQueryAggregateTarget) Kind() domain.ErrorKind { return domain.KindValidation }

// ErrQueryModeUnsupported rejects a QueryMode this build does not implement —
// a caller sending an enum value from a newer protocol revision, or an
// out-of-range number. The request names something the server cannot execute,
// which is an argument error rather than a server fault.
type ErrQueryModeUnsupported struct {
	Mode commonpb.QueryMode
}

func (e *ErrQueryModeUnsupported) Error() string {
	return fmt.Sprintf("unsupported query mode: %v", e.Mode)
}

func (*ErrQueryModeUnsupported) Kind() domain.ErrorKind { return domain.KindValidation }

var (
	_ domain.Classifiable = (*ErrPreparedQueryAggregateTarget)(nil)
	_ domain.Classifiable = (*ErrQueryModeUnsupported)(nil)
)
