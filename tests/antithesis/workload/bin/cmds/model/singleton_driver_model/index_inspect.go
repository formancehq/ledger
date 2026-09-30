package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// InspectIndex reads the metadata index a filtered query is served from, so it
// answers from the same projection and is gated by the same readiness.
//
// Every mode is judged on its page contract — length, one entry per value, a
// resume cursor published exactly when the scan has more, a facet counting at
// least the group that produced it. Summary is judged further: its three
// counters are folded from the model exactly (see modelInspectCounts).
//
// The paged modes' VALUES are not compared. Predicting which slice of the value
// space lands on a page needs the index's own order-preserving encoding per
// declared type, which the model does not reproduce; the counters need no
// ordering, so they are exact.
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

	// Summary carries the only arm held to the model, and a successful
	// inspection is already rare — the index has to be built and the key
	// declared — so it takes half the rolls rather than a third.
	mode := random.RandomChoice([]servicepb.InspectIndexMode{
		servicepb.InspectIndexMode_INSPECT_INDEX_MODE_DISTINCT_VALUES,
		servicepb.InspectIndexMode_INSPECT_INDEX_MODE_FACETS,
		servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY,
		servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY,
	})
	requestedPageSize, pageSize := queryPageSize()
	noteClampedPageSize(requestedPageSize, pageSize)

	// A frozen checkpoint answers from its own index snapshot; a checkpoint the
	// fleet has since dropped answers NotFound.
	var checkpointID uint64
	if oneIn(4) {
		c.mu.Lock()
		checkpointID, _, _ = c.pickCheckpointReadTarget()
		c.mu.Unlock()
	}

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	responseFrontier := c.beginResponseFrontier()

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	request := &servicepb.InspectIndexRequest{
		Ledger:       ledger,
		TargetType:   target,
		MetadataKey:  key,
		Mode:         mode,
		PageSize:     uint32(requestedPageSize),
		CheckpointId: checkpointID,
	}

	res, err := client.InspectIndex(readCtx, request)

	maxTicket := responseFrontier()

	if err != nil {
		if internal.IsTransient(err) && !isIndexNotReady(err) || isShutdownError(err) {
			return
		}

		if checkpointID != 0 && checkpointNotFound(err) {
			// A checkpoint the fleet dropped cannot answer; the live path's own
			// checks cover everything this read would have proved.
			return
		}

		if status.Code(err) == codes.NotFound {
			if absent {
				// Coverage: inspecting a ledger outside the fleet must say NotFound.
				assert.Reachable("singleton_driver_model: index inspection on an absent ledger returned NotFound", internal.Details{"ledger": ledger})

				return
			}

			c.validateLedgerNotFound(maxTicket, ledger, "InspectIndex")

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

	if mode == servicepb.InspectIndexMode_INSPECT_INDEX_MODE_SUMMARY && checkpointID == 0 {
		if !c.validateInspectSummary(maxTicket, ledger, queryTarget, key, declared, res.GetSummary(), details) {
			return
		}
	}

	// Coverage: an inspection answered and held its page contract.
	assert.Reachable("singleton_driver_model: index inspection validated", internal.Details{"mode": mode.String()})

	if checkpointID != 0 {
		// Coverage: the inspection answered from a frozen checkpoint's index.
		assert.Reachable("singleton_driver_model: index inspection served from a checkpoint", internal.Details{"checkpoint": checkpointID})
	}

	c.resumeInspectIndex(readCtx, client, request, res, mode, pageSize, details)
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

// resumeInspectIndex follows a published resume cursor once, holding the second
// page to the same contract. The cursor is opaque, so it is round-tripped rather
// than synthesized.
func (c *Checker) resumeInspectIndex(
	ctx context.Context,
	client servicepb.BucketServiceClient,
	request *servicepb.InspectIndexRequest,
	first *servicepb.InspectIndexResponse,
	mode servicepb.InspectIndexMode,
	pageSize int,
	details internal.Details,
) {
	next := first.GetDistinctValues().GetNextCursor()
	if next == "" {
		next = first.GetFacets().GetNextCursor()
	}

	if next == "" {
		return
	}

	request.Cursor = next

	res, err := client.InspectIndex(ctx, request)
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) || isIndexNotReady(err) ||
			isIndexNotFound(err) || status.Code(err) == codes.NotFound {
			return
		}

		details["cursor"], details["error"] = next, err.Error()
		assert.Unreachable("singleton_driver_model: resumed index inspection returned unexpected error", details)

		return
	}

	if why := inspectPageViolation(res, mode, pageSize); why != "" {
		details["cursor"], details["why"] = next, why
		assert.Unreachable("singleton_driver_model: resumed index inspection violates its own contract", details)

		return
	}

	// Coverage: a second page was fetched with the cursor the first published.
	assert.Reachable("singleton_driver_model: index inspection resumed from its cursor", internal.Details{"mode": mode.String()})
}

// inspectCounts is what a summary inspection reports about one (target, key).
type inspectCounts struct {
	cardinality      uint64
	entitiesWithKey  uint64
	entitiesWithNull uint64
}

// modelInspectCounts folds a base's metadata for one (target, key) the way the
// index writer keys it: every entity carrying the key contributes exactly one
// encoded value, coerced to the declared type first, and the distinct encodings
// are the value index's groups.
//
// A value that does not survive the coercion becomes a null encoding, and the
// encoder carries the text it failed to convert into the key
// (readstore.EncodeNull), so two entities holding different unconvertible text
// are two groups, not one. Those entities are counted by entities_with_null;
// entities_with_key counts only the rest. That split is why cardinality does
// not bound entities_with_key on its own — a key every entity holds
// unconvertibly reports entities_with_key 0 with a non-zero cardinality.
func modelInspectCounts(ls oracle.LedgerState, target commonpb.QueryTarget, key string, declared commonpb.MetadataType) inspectCounts {
	var counts inspectCounts

	groups := map[string]struct{}{}

	fold := func(stored *commonpb.MetadataValue) {
		coerced := stored
		if !commonpb.TypeMatches(stored, declared) {
			coerced = commonpb.ConvertMetadataValue(stored, declared)
		}

		groups[oracle.MetaValueString(coerced)] = struct{}{}

		if _, isNull := coerced.GetType().(*commonpb.MetadataValue_NullValue); isNull {
			counts.entitiesWithNull++

			return
		}

		counts.entitiesWithKey++
	}

	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		txs := ls.Txs()
		for i := range txs.Len() {
			if v, ok := txs.Get(i).Metadata()[key]; ok {
				fold(v)
			}
		}
	} else {
		for k, v := range ls.Metadata().All() {
			if k.Key == key {
				fold(v)
			}
		}
	}

	counts.cardinality = uint64(len(groups))

	return counts
}

// validateInspectSummary holds a summary's three counters to the model. Returns
// false once it has reported a finding.
//
// A retype rewrites the keyspace under a new encoding while the resolver may
// still serve the version it replaced, so the counters are read against the old
// declared type for as long as that window is open and are not judged.
func (c *Checker) validateInspectSummary(
	maxTicket uint64,
	ledger string,
	target commonpb.QueryTarget,
	key string,
	declared commonpb.MetadataType,
	summary *servicepb.InspectSummary,
	details internal.Details,
) bool {
	if summary == nil {
		assert.Unreachable("singleton_driver_model: summary mode answered another arm", details)

		return false
	}

	served := inspectCounts{
		cardinality:      summary.GetCardinality(),
		entitiesWithKey:  summary.GetEntitiesWithKey(),
		entitiesWithNull: summary.GetEntitiesWithNull(),
	}

	canonical := metadataCanonical(target, key)

	if c.matchesModel(maxTicket, "INSPECTSUMMARY", func(base oracle.GlobalState) bool {
		ls, live := liveLedgerState(base, ledger)
		if !live {
			return false
		}

		if _, open := ls.RetypeWindow(canonical); open {
			return true
		}

		return modelInspectCounts(ls, target, key, declared) == served
	}) {
		// Coverage: a summary's counters were the model's own fold of the key.
		// An empty index agrees on zeroes whatever the fold does, so the
		// populated case is its own fact — that is the one that proves the
		// comparison has teeth.
		assert.Reachable("singleton_driver_model: index inspection summary matched the model", internal.Details{
			"cardinality": served.cardinality,
		})

		if served.cardinality > 0 {
			assert.Reachable("singleton_driver_model: index inspection summary matched a populated index", internal.Details{
				"cardinality": served.cardinality,
				"withKey":     served.entitiesWithKey,
				"withNull":    served.entitiesWithNull,
			})
		}

		if served.entitiesWithNull > 0 {
			// Coverage: the null split is exercised — a value the declared type
			// could not hold, counted apart from the carriers.
			assert.Reachable("singleton_driver_model: index inspection summary counted unconvertible values", internal.Details{
				"withNull": served.entitiesWithNull,
			})
		}

		return true
	}

	details["servedCardinality"] = served.cardinality
	details["servedWithKey"] = served.entitiesWithKey
	details["servedWithNull"] = served.entitiesWithNull
	details["modelCounts"] = c.modelInspectCountsDump(ledger, target, key, declared)
	assert.Unreachable("singleton_driver_model: index inspection summary outside model", details)

	return false
}

// modelInspectCountsDump renders the committed model's fold for a finding.
// Acquires c.mu.
func (c *Checker) modelInspectCountsDump(ledger string, target commonpb.QueryTarget, key string, declared commonpb.MetadataType) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	counts := modelInspectCounts(c.modelState.Ledger(ledger), target, key, declared)

	return fmt.Sprintf("cardinality=%d withKey=%d withNull=%d",
		counts.cardinality, counts.entitiesWithKey, counts.entitiesWithNull)
}
