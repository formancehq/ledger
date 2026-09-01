package main

import (
	"context"
	"sort"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// The three registry read RPCs answer from the index registry, an FSM
// projection of the CreateIndex / DropIndex / RemoveMetadataFieldType log
// stream — exactly the state the model's index map tracks, cascade included. So
// registry membership is checked for equality, not bounded.
//
// What is NOT checked, and why: build_status is informational and advances with
// the backfill; created_at is server-stamped; and IndexEntry's cursor,
// current_version and pending_version come from a readstore snapshot taken
// beside the main-store handle, so they belong to a different fold point than
// the registry entry they arrive with. current_version against the registry's
// forward_encoding_version would be a cross-store comparison for the same
// reason, and nothing documents pending_version > current_version as an
// invariant, so neither relation is asserted.
func runIndexIntrospection(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	switch random.RandomChoice([]uint8{0, 1, 2}) {
	case 0:
		runIndexList(ctx, client, c)
	case 1:
		runIndexGet(ctx, client, c)
	default:
		runIndexEntryStatus(ctx, client, c)
	}
}

// pickIntrospectIndex chooses an index id to look up: one the workload churns,
// one of the ledger's declared metadata fields (whose registry entry the removal
// cascade takes with the declaration), or — one in four — an id the workload
// never registers, so the NotFound side is reached as often as the served one.
func pickIntrospectIndex(c *Checker, ledger string) *commonpb.IndexID {
	if oneIn(4) {
		return indexes.MetadataID(commonpb.TargetType_TARGET_TYPE_ACCOUNT, "unregistered-"+metaKey())
	}

	c.mu.Lock()
	ls := c.modelState.Ledger(ledger)
	c.mu.Unlock()

	// A published state is never mutated, so the declared-field walk is safe
	// outside the lock.
	all := workloadIndexes()

	for _, target := range []commonpb.TargetType{
		commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		commonpb.TargetType_TARGET_TYPE_TRANSACTION,
	} {
		for key := range ls.FieldTypesFor(target).All() {
			id := indexes.MetadataID(target, key)
			all = append(all, workloadIndex{id, indexes.Canonical(id)})
		}
	}

	return all[internal.Rand().Intn(len(all))].id
}

// runIndexList streams a ledger's registry entries and checks the canonical set
// against the model. SCOPE_LEDGER filters on an exact ledger match and its only
// orphan skipping is for entries of deleted ledgers, which the never-deleted
// fleet cannot produce — so the served set is the ledger's whole registry and an
// equality check is exact.
func runIndexList(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(c.ledgerNames, 2)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	stream, err := client.ListIndexes(readCtx, &servicepb.ListIndexesRequest{
		Scope:  servicepb.ListIndexesRequest_SCOPE_LEDGER,
		Ledger: ledger,
	})

	var entries []*commonpb.Index
	if err == nil {
		entries, err = drainStream(stream)
	}

	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}

		if absent && status.Code(err) == codes.NotFound {
			// Coverage: SCOPE_LEDGER on an unknown ledger must say NotFound rather
			// than answer with an empty listing.
			assert.Reachable("singleton_driver_model: index listing on an absent ledger returned NotFound", internal.Details{"ledger": ledger})

			return
		}

		assert.Unreachable("singleton_driver_model: ListIndexes returned unexpected error", internal.Details{
			"ledger": ledger,
			"absent": absent,
			"error":  err.Error(),
		})

		return
	}

	if absent {
		assert.Unreachable("singleton_driver_model: index listing served a ledger outside the fleet", internal.Details{
			"ledger": ledger,
			"count":  len(entries),
		})

		return
	}

	served := make(map[string]bool, len(entries))

	for _, idx := range entries {
		if idx.GetId() == nil || idx.GetLedger() != ledger {
			assert.Unreachable("singleton_driver_model: index listing violated its scope", internal.Details{
				"ledger":      ledger,
				"entryLedger": idx.GetLedger(),
				"hasID":       idx.GetId() != nil,
			})

			return
		}

		canonical := indexes.Canonical(idx.GetId())
		if served[canonical] {
			assert.Unreachable("singleton_driver_model: index listing repeated an entry", internal.Details{
				"ledger": ledger,
				"index":  canonical,
			})

			return
		}

		served[canonical] = true
	}

	if c.matchesModel(maxTicket, "INDEXLIST", func(base oracle.GlobalState) bool {
		return indexSetEqual(base.Ledger(ledger), served)
	}) {
		// Coverage: a registry listing matched the model's declared set.
		assert.Reachable("singleton_driver_model: index listing validated", internal.Details{
			"ledger": ledger,
			"count":  len(served),
		})

		return
	}

	assert.Unreachable("singleton_driver_model: index listing outside model", internal.Details{
		"ledger":       ledger,
		"serverIdx":    joinSortedKeys(served),
		"modelIndexes": c.modelIndexList(ledger),
		"foldDiag":     c.foldDiag(maxTicket),
	})
}

// indexSetEqual reports whether a base's declared index set is exactly served.
func indexSetEqual(ls oracle.LedgerState, served map[string]bool) bool {
	declared := 0

	for canonical := range ls.Indexes().All() {
		if !served[canonical] {
			return false
		}

		declared++
	}

	return declared == len(served)
}

// runIndexGet reads one registry entry by id.
func runIndexGet(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(c.ledgerNames, 2)
	id := pickIntrospectIndex(c, ledger)
	canonical := indexes.Canonical(id)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	idx, err := client.GetIndex(readCtx, &servicepb.GetIndexRequest{Ledger: ledger, Id: id})

	maxTicket := c.ticketSeq.Load()

	found, ok := c.classifyIndexLookup("GetIndex", ledger, canonical, absent, err)
	if !ok {
		return
	}

	if found && (idx.GetLedger() != ledger || indexes.Canonical(idx.GetId()) != canonical) {
		assert.Unreachable("singleton_driver_model: index lookup answered with another entry", internal.Details{
			"rpc":          "GetIndex",
			"ledger":       ledger,
			"askedIndex":   canonical,
			"servedLedger": idx.GetLedger(),
			"servedIndex":  indexes.Canonical(idx.GetId()),
		})

		return
	}

	if c.validateIndexPresence(maxTicket, "GetIndex", ledger, canonical, found) {
		// Coverage: a registry point read agreed with the model on presence.
		assert.Reachable("singleton_driver_model: index lookup validated", internal.Details{
			"ledger": ledger,
			"index":  canonical,
			"found":  found,
		})
	}
}

// runIndexEntryStatus reads one index's status view. Only the registry entry it
// carries is checked; the backfill cursor and per-replica versions beside it come
// from a readstore snapshot at its own fold point.
func runIndexEntryStatus(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(c.ledgerNames, 2)
	id := pickIntrospectIndex(c, ledger)
	canonical := indexes.Canonical(id)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	entry, err := client.GetIndexEntryStatus(readCtx, &servicepb.GetIndexEntryStatusRequest{Ledger: ledger, Id: id})

	maxTicket := c.ticketSeq.Load()

	found, ok := c.classifyIndexLookup("GetIndexEntryStatus", ledger, canonical, absent, err)
	if !ok {
		return
	}

	if found && (entry.GetLedger() != ledger || indexes.Canonical(entry.GetIndex().GetId()) != canonical) {
		assert.Unreachable("singleton_driver_model: index lookup answered with another entry", internal.Details{
			"rpc":          "GetIndexEntryStatus",
			"ledger":       ledger,
			"askedIndex":   canonical,
			"servedLedger": entry.GetLedger(),
			"servedIndex":  indexes.Canonical(entry.GetIndex().GetId()),
		})

		return
	}

	if c.validateIndexPresence(maxTicket, "GetIndexEntryStatus", ledger, canonical, found) {
		// Coverage: a status view agreed with the model on presence.
		assert.Reachable("singleton_driver_model: index status validated", internal.Details{
			"ledger": ledger,
			"index":  canonical,
			"found":  found,
		})
	}
}

// classifyIndexLookup turns a registry point read's outcome into "the server
// says the entry is there". A NotFound is ambiguous on the wire — the ledger or
// the index can be the missing one — so an absent-ledger probe consumes it here
// and only a fleet ledger's answer reaches the presence check. ok=false means the
// outcome was handled (a transient, a shutdown, or a finding already raised).
func (c *Checker) classifyIndexLookup(rpc, ledger, canonical string, absent bool, err error) (found, ok bool) {
	if err == nil {
		if absent {
			assert.Unreachable("singleton_driver_model: index lookup served a ledger outside the fleet", internal.Details{
				"rpc":    rpc,
				"ledger": ledger,
				"index":  canonical,
			})

			return false, false
		}

		return true, true
	}

	if internal.IsTransient(err) || isShutdownError(err) {
		return false, false
	}

	if status.Code(err) != codes.NotFound {
		assert.Unreachable("singleton_driver_model: index lookup returned unexpected error", internal.Details{
			"rpc":    rpc,
			"ledger": ledger,
			"index":  canonical,
			"absent": absent,
			"error":  err.Error(),
		})

		return false, false
	}

	if absent {
		// Coverage: a ledger outside the fleet must resolve NotFound.
		assert.Reachable("singleton_driver_model: index lookup on an absent ledger returned NotFound", internal.Details{
			"rpc":    rpc,
			"ledger": ledger,
		})

		return false, false
	}

	return false, true
}

// validateIndexPresence checks a registry point read against the candidate
// bases. Both directions are findings: an entry served for an index no base
// declares is a projection row the log stream never authorised, and a NotFound
// for one every base declares is a row the registry lost.
func (c *Checker) validateIndexPresence(maxTicket uint64, rpc, ledger, canonical string, found bool) bool {
	if c.matchesModel(maxTicket, "INDEXPRESENCE", func(base oracle.GlobalState) bool {
		exists, _ := base.Ledger(ledger).IndexState(canonical)

		return exists == found
	}) {
		return true
	}

	assert.Unreachable("singleton_driver_model: index presence outside model", internal.Details{
		"rpc":          rpc,
		"ledger":       ledger,
		"index":        canonical,
		"servedFound":  found,
		"modelIndexes": c.modelIndexList(ledger),
		"foldDiag":     c.foldDiag(maxTicket),
	})

	return false
}

// modelIndexList renders the committed declared index set for finding
// diagnostics. Acquires c.mu.
func (c *Checker) modelIndexList(ledger string) string {
	c.mu.Lock()
	defer c.mu.Unlock()

	var out []string
	for canonical := range c.modelState.Ledger(ledger).Indexes().All() {
		out = append(out, canonical)
	}

	sort.Strings(out)

	return strings.Join(out, ",")
}

// joinSortedKeys renders a canonical set in a stable order.
func joinSortedKeys(set map[string]bool) string {
	out := make([]string, 0, len(set))
	for k := range set {
		out = append(out, k)
	}

	sort.Strings(out)

	return strings.Join(out, ",")
}
