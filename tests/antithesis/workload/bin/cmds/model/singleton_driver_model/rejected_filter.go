package main

import (
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/pkg/proto/v3/ledgerpb"
	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// rejectedFilter describes a filter the server must refuse whatever the
// ledger's state: some node fails the per-target validity or shape checks the
// compiler runs at dispatch, the tree is too deep, or a leaf references a
// parameter, which a plain list query never binds.
type rejectedFilter struct {
	rejected bool
	// prior maps each index the leaves compiled before the first refused one
	// need to the label the server names it by. The compiler walks the tree in
	// order and checks each leaf's index as it compiles it, so a not-ready
	// rejection from one of these may come first.
	prior map[string]priorIndex
}

type priorIndex struct {
	// label is the server's ErrorInfo "index" label; empty when unknown.
	label string
	// metadata is set for a metadata index: under an open retype window it may
	// answer not-ready even while active.
	metadata bool
}

// classifyRejectedFilter decides whether f must be refused on target, and which
// indexes may answer not-ready before the refusal is reached.
func classifyRejectedFilter(f *ledgerpb.QueryFilter, target ledgerpb.QueryTarget) rejectedFilter {
	if domain.ValidateFilterForTarget(f, target) == nil && len(collectParams(f)) == 0 {
		return rejectedFilter{}
	}

	prior := map[string]priorIndex{}
	stopped := false
	visitLeaves(f, func(leaf *ledgerpb.QueryFilter) {
		if stopped {
			return
		}
		if leaf != nil && (!ledgerpb.ConditionValidForTarget(target, ledgerpb.ConditionKindOf(leaf)) || domain.ValidateFilterLeaf(leaf) != nil) {
			stopped = true

			return
		}

		// A leaf checks its index before it resolves its parameters.
		needed := map[string]struct{}{}
		if target == ledgerpb.QueryTarget_QUERY_TARGET_LOGS {
			needed = neededLogIndexes(leaf)
		} else {
			neededIndexCanonicals(leaf, target, needed)
		}
		for canon := range needed {
			prior[canon] = priorIndex{
				label:    serverIndexLabel(leaf, target),
				metadata: leaf.GetField().GetField().GetMetadata() != "",
			}
		}
		stopped = len(collectParams(leaf)) > 0
	})

	return rejectedFilter{rejected: true, prior: prior}
}

// serverIndexLabel is the label the compiler names leaf's index by in an
// INDEX_NOT_FOUND / INDEX_BUILDING ErrorInfo (internal/query/compile.go).
func serverIndexLabel(leaf *ledgerpb.QueryFilter, target ledgerpb.QueryTarget) string {
	if target == ledgerpb.QueryTarget_QUERY_TARGET_LOGS {
		return "log date"
	}

	switch x := leaf.GetFilter().(type) {
	case *ledgerpb.QueryFilter_Field:
		return fmt.Sprintf("metadata[%q] on %s", x.Field.GetField().GetMetadata(), ledgerpb.TargetHumanName(target))
	case *ledgerpb.QueryFilter_AccountHasAsset:
		return "has asset"
	case *ledgerpb.QueryFilter_Reference:
		return "reference"
	case *ledgerpb.QueryFilter_Address:
		switch x.Address.GetRole() {
		case ledgerpb.AddressRole_ADDRESS_ROLE_SOURCE:
			return "source"
		case ledgerpb.AddressRole_ADDRESS_ROLE_DESTINATION:
			return "destination"
		default:
			return "address"
		}
	case *ledgerpb.QueryFilter_BuiltinUint:
		switch x.BuiltinUint.GetField() {
		case ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
			return "timestamp"
		case ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
			return "inserted_at"
		case ledgerpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT:
			return "reverted_at"
		}
	}

	return ""
}

// handleRejectedFilterError validates the error a refused filter drew: the
// InvalidArgument refusal, or a not-ready rejection from an index an earlier
// leaf needs while some candidate base does not hold it active. Returns true
// when it has fully handled err.
func (c *Checker) handleRejectedFilterError(maxTicket uint64, rf rejectedFilter, target ledgerpb.QueryTarget, ledger string, filter *ledgerpb.QueryFilter, err error) bool {
	if !rf.rejected {
		return false
	}

	if status.Code(err) == codes.InvalidArgument {
		// Coverage: one literal per target — Antithesis catalogues assertions by
		// literal.
		switch target {
		case ledgerpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
			assert.Reachable("singleton_driver_model: refused account filter rejected", internal.Details{"ledger": ledger})
		case ledgerpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
			assert.Reachable("singleton_driver_model: refused transaction filter rejected", internal.Details{"ledger": ledger})
		default:
			assert.Reachable("singleton_driver_model: refused log filter rejected", internal.Details{"ledger": ledger})
		}

		return true
	}

	if (isIndexNotReady(err) || isIndexNotFound(err)) && c.matchesModel(maxTicket, "REFUSED-FILTER", func(base oracle.GlobalState) bool {
		ls, live := liveLedgerState(base, ledger)

		return live && refusedFilterNotReadyLegal(ls, rf, target, err)
	}) {
		return true
	}

	assert.Unreachable("singleton_driver_model: refused filter returned unexpected error", internal.Details{
		"target": target.String(),
		"ledger": ledger,
		"filter": describeFilter(filter),
		"error":  err.Error(),
	})

	return true
}

// assertRefusedFilterServedNothing fires when a filter the server must refuse
// produced a page. Returns true when rf is a refused filter.
func assertRefusedFilterServedNothing(rf rejectedFilter, target ledgerpb.QueryTarget, ledger string, filter *ledgerpb.QueryFilter, rows int) bool {
	if !rf.rejected {
		return false
	}

	assert.Unreachable("singleton_driver_model: refused filter returned results", internal.Details{
		"target": target.String(),
		"ledger": ledger,
		"filter": describeFilter(filter),
		"rows":   rows,
	})

	return true
}

// refusedFilterNotReadyLegal reports whether ls explains a not-ready answer to
// a refused filter: the refusal names an index a leaf compiled before the
// refused one needs, and that index is absent or not active, or is a metadata
// index under an open retype window. A refusal naming any other index stays
// illegal.
func refusedFilterNotReadyLegal(ls oracle.LedgerState, rf rejectedFilter, target ledgerpb.QueryTarget, err error) bool {
	rejectedIndex := rejectedIndexLabel(err)
	for canon, p := range rf.prior {
		if p.label != "" && p.label != rejectedIndex {
			continue
		}

		if exists, active := ls.IndexState(canon); !exists || !active {
			return true
		}

		if _, open := ls.RetypeWindow(canon); p.metadata && open && isIndexNotReady(err) {
			return true
		}
	}

	return false
}
