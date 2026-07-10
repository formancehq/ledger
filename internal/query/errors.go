package query

import (
	"fmt"

	"github.com/formancehq/ledger/v3/internal/domain"
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
// business outcomes). It still implements domain.Describable so it flows
// through the shared error pipeline to the gRPC/HTTP adapters, and it reuses
// the domain-level wire constant domain.ErrReasonAggregateOverflow (the
// client-facing reason string and its KindForReason classification are the
// shared wire contract and stay in domain).
type ErrAggregateOverflow struct {
	Stage string // "accumulate", "collapse-colors", "max-precision-merge" or "max-precision-rescale"
	Side  string // "input" or "output"
}

func (e *ErrAggregateOverflow) Error() string {
	return fmt.Sprintf("aggregate volume %s overflowed 2^256 during %s", e.Side, e.Stage)
}
func (*ErrAggregateOverflow) Reason() string { return domain.ErrReasonAggregateOverflow }
func (e *ErrAggregateOverflow) Metadata() map[string]string {
	return map[string]string{"stage": e.Stage, "side": e.Side}
}

// Compile-time assertion that ErrAggregateOverflow satisfies domain.Describable
// so it keeps flowing through the shared error edge (gRPC/HTTP mapping).
var _ domain.Describable = (*ErrAggregateOverflow)(nil)
