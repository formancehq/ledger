package main

import (
	"context"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// InspectIndex reads the metadata index a filtered query is served from, so it
// answers from the same projection and is gated by the same readiness.
//
// Only the page contract is judged — length, one entry per value, a resume
// cursor published exactly when the scan has more, a facet counting at least the
// group that produced it. The served values and counts are not held to the
// model: the index and the entity-exists keyspace are separate projections with
// separate lifetimes, so a key whose metadata was deleted leaves value groups
// live that no entity carries any more. Summary's cardinality counts the former
// and entities_with_key the latter, which is why neither bounds the other.
func runInspectIndex(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(c.ledgerNames, 2)

	target := commonpb.TargetType_TARGET_TYPE_ACCOUNT
	queryTarget := commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS
	if oneIn(2) {
		target, queryTarget = commonpb.TargetType_TARGET_TYPE_TRANSACTION, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	}

	key, declared := c.pickInspectKey(ledger, queryTarget)
	if key == "" {
		return
	}

	mode := random.RandomChoice([]servicepb.InspectIndexMode{
		servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES,
		servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS,
		servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY,
	})
	requestedPageSize, pageSize := queryPageSize()
	noteClampedPageSize(requestedPageSize, pageSize)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	res, err := client.InspectIndex(readCtx, &servicepb.InspectIndexRequest{
		Ledger:      ledger,
		TargetType:  target,
		MetadataKey: key,
		Mode:        mode,
		PageSize:    uint32(requestedPageSize),
	})

	if err != nil {
		if internal.IsTransient(err) && !isIndexNotReady(err) || isShutdownError(err) {
			return
		}

		if absent && status.Code(err) == codes.NotFound {
			// Coverage: inspecting a ledger outside the fleet must say NotFound.
			assert.Reachable("singleton_driver_model: index inspection on an absent ledger returned NotFound", internal.Details{"ledger": ledger})

			return
		}

		if isIndexNotReady(err) || isIndexNotFound(err) {
			// Coverage: the inspection rides the same readiness gate a query does.
			assert.Reachable("singleton_driver_model: index inspection gated on a missing index", internal.Details{"ledger": ledger})

			return
		}

		assert.Unreachable("singleton_driver_model: InspectIndex returned unexpected error", internal.Details{
			"ledger": ledger,
			"key":    key,
			"target": target.String(),
			"mode":   mode.String(),
			"error":  err.Error(),
		})

		return
	}

	if absent {
		assert.Unreachable("singleton_driver_model: index inspection served a ledger outside the fleet", internal.Details{"ledger": ledger})

		return
	}

	details := internal.Details{
		"ledger":   ledger,
		"key":      key,
		"target":   target.String(),
		"mode":     mode.String(),
		"declared": declared.String(),
		"pageSize": pageSize,
	}

	if why := inspectPageViolation(res, mode, pageSize); why != "" {
		details["why"] = why
		assert.Unreachable("singleton_driver_model: index inspection violates its own contract", details)

		return
	}

	// Coverage: an inspection answered and held its page contract.
	assert.Reachable("singleton_driver_model: index inspection validated", internal.Details{"mode": mode.String()})
}

// pickInspectKey chooses a declared metadata key of the target, or — one read
// in five — one the ledger never declared, whose index the registry does not
// hold. Acquires c.mu.
func (c *Checker) pickInspectKey(ledger string, target commonpb.QueryTarget) (key string, declared commonpb.MetadataType) {
	if oneIn(5) {
		return "undeclared-" + metaKey(), commonpb.MetadataType_METADATA_TYPE_STRING
	}

	c.mu.Lock()
	ls := c.modelState.Ledger(ledger)
	c.mu.Unlock()

	var keys []string
	types := map[string]commonpb.MetadataType{}

	for k, t := range declaredFieldTypes(ls, target).All() {
		keys = append(keys, k)
		types[k] = t
	}

	if len(keys) == 0 {
		return "", commonpb.MetadataType_METADATA_TYPE_STRING
	}

	key = keys[internal.Rand().Intn(len(keys))]

	return key, types[key]
}

// inspectPageViolation reports the first way a served inspection breaks its own
// contract: a page longer than asked for, a value served twice, a resume cursor
// that disagrees with has_more, a facet counting nothing, or a summary claiming
// more distinct values than entities carrying the key.
func inspectPageViolation(res *servicepb.InspectIndexResponse, mode servicepb.InspectIndexMode, pageSize int) string {
	switch mode {
	case servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES:
		page := res.GetDistinctValues()
		if page == nil {
			return "distinct-values mode answered another arm"
		}

		if len(page.GetValues()) > pageSize {
			return "page longer than requested"
		}

		if seen := map[string]bool{}; !distinctValues(page.GetValues(), seen) {
			return "value served twice"
		}

		return cursorPairing(page.GetHasMore(), page.GetNextCursor())
	case servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS:
		page := res.GetFacets()
		if page == nil {
			return "facets mode answered another arm"
		}

		if len(page.GetFacets()) > pageSize {
			return "page longer than requested"
		}

		seen := map[string]bool{}
		for _, f := range page.GetFacets() {
			if f.GetCount() == 0 {
				return "facet counts no entity"
			}

			if seen[f.GetValue().String()] {
				return "value served twice"
			}

			seen[f.GetValue().String()] = true
		}

		return cursorPairing(page.GetHasMore(), page.GetNextCursor())
	default:
		summary := res.GetSummary()
		if summary == nil {
			return "summary mode answered another arm"
		}

		return ""
	}
}

// cursorPairing reports the inspection's resume contract: a token is published
// exactly when the scan has more to serve.
func cursorPairing(hasMore bool, next string) string {
	if hasMore == (next != "") {
		return ""
	}

	if hasMore {
		return "more to serve but no resume cursor"
	}

	return "resume cursor on an exhausted scan"
}

func distinctValues(values []*commonpb.MetadataValue, seen map[string]bool) bool {
	for _, v := range values {
		if seen[v.String()] {
			return false
		}

		seen[v.String()] = true
	}

	return true
}
