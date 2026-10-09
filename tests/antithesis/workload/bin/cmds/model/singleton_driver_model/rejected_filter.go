package main

import (
	"fmt"

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
	// priorKeys are the metadata keys those leaves read: a key under an open
	// retype window may answer not-ready even while its index is active.
	priorKeys []string
}

// classifyRejectedFilter decides whether f must be refused on target, and which
// indexes may answer not-ready before the refusal is reached.
func classifyRejectedFilter(f *commonpb.QueryFilter, target commonpb.QueryTarget) rejectedFilter {
	if domain.ValidateFilterForTarget(f, target) == nil && len(collectParams(f)) == 0 {
		return rejectedFilter{}
	}

	prior := map[string]struct{}{}
	var priorKeys []string
	stopped := false
	visitLeaves(f, func(leaf *commonpb.QueryFilter) {
		if stopped {
			return
		}
		if leaf != nil && (!commonpb.ConditionValidForTarget(target, commonpb.ConditionKindOf(leaf)) || domain.ValidateFilterLeaf(leaf) != nil) {
			stopped = true

			return
		}

		if key := leaf.GetField().GetField().GetMetadata(); key != "" {
			priorKeys = append(priorKeys, key)
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

	return rejectedFilter{rejected: true, prior: prior, priorKeys: priorKeys}
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

// refusedFilterNotReadyLegal reports whether ls explains a not-ready answer to
// a refused filter: an index a leaf compiled before the refused one needs is
// absent or not active, or a key such a leaf reads is under an open retype
// window and the refusal names that key's index itself — an active sibling's
// refusal stays illegal.
func refusedFilterNotReadyLegal(ls oracle.LedgerState, rf rejectedFilter, target commonpb.QueryTarget, err error) bool {
	for canon := range rf.prior {
		if exists, active := ls.IndexState(canon); !exists || !active {
			return true
		}
	}

	if !isIndexNotReady(err) {
		return false
	}

	rejectedIndex := rejectedIndexLabel(err)
	for _, key := range rf.priorKeys {
		_, open := ls.RetypeWindow(metadataCanonical(target, key))
		if open && rejectedIndex == fmt.Sprintf("metadata[%q] on %s", key, commonpb.TargetHumanName(target)) {
			return true
		}
	}

	return false
}
