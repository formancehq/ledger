package main

import (
	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// rejectedFilter describes a filter the server must refuse whatever the
// ledger's state: some node fails the per-target validity or shape checks the
// compiler runs at dispatch, the tree is too deep, or a leaf references a
// parameter, which a plain list query never binds.
type rejectedFilter struct {
	rejected bool
	// prior names the indexes the leaves compiled before the first refused one
	// need. The compiler walks the tree in order and checks each leaf's index as
	// it compiles it, so a not-ready rejection from one of these may come first.
	prior map[string]struct{}
}

// classifyRejectedFilter decides whether f must be refused on target, and which
// indexes may answer not-ready before the refusal is reached.
func classifyRejectedFilter(f *commonpb.QueryFilter, target commonpb.QueryTarget) rejectedFilter {
	if domain.ValidateFilterForTarget(f, target) == nil && len(collectParams(f)) == 0 {
		return rejectedFilter{}
	}

	prior := map[string]struct{}{}
	stopped := false
	visitLeaves(f, func(leaf *commonpb.QueryFilter) {
		if stopped {
			return
		}
		if leaf != nil && (!commonpb.ConditionValidForTarget(target, commonpb.ConditionKindOf(leaf)) || domain.ValidateFilterLeaf(leaf) != nil) {
			stopped = true

			return
		}

		// A leaf checks its index before it resolves its parameters.
		if target == commonpb.QueryTarget_QUERY_TARGET_LOGS {
			for canon := range neededLogIndexes(leaf) {
				prior[canon] = struct{}{}
			}
		} else {
			neededIndexCanonicals(leaf, target, prior)
		}
		stopped = len(collectParams(leaf)) > 0
	})

	return rejectedFilter{rejected: true, prior: prior}
}

// handleRejectedFilterError validates the error a refused filter drew: the
// InvalidArgument refusal, or a not-ready rejection from an index an earlier
// leaf needs while some candidate base does not hold it active. Returns true
// when it has fully handled err.
func (c *Checker) handleRejectedFilterError(maxTicket uint64, rf rejectedFilter, target commonpb.QueryTarget, ledger string, filter *commonpb.QueryFilter, err error) bool {
	if !rf.rejected {
		return false
	}

	if status.Code(err) == codes.InvalidArgument {
		// Coverage: one literal per target — Antithesis catalogues assertions by
		// literal.
		switch target {
		case commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS:
			assert.Reachable("singleton_driver_model: refused account filter rejected", internal.Details{"ledger": ledger})
		case commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS:
			assert.Reachable("singleton_driver_model: refused transaction filter rejected", internal.Details{"ledger": ledger})
		default:
			assert.Reachable("singleton_driver_model: refused log filter rejected", internal.Details{"ledger": ledger})
		}

		return true
	}

	if (isIndexNotReady(err) || isIndexNotFound(err)) && c.matchesModel(maxTicket, "REFUSED-FILTER", func(base oracle.GlobalState) bool {
		ls, live := liveLedgerState(base, ledger)
		if !live {
			return false
		}
		for canon := range rf.prior {
			if exists, active := ls.IndexState(canon); !exists || !active {
				return true
			}
		}

		return false
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
func assertRefusedFilterServedNothing(rf rejectedFilter, target commonpb.QueryTarget, ledger string, filter *commonpb.QueryFilter, rows int) bool {
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
