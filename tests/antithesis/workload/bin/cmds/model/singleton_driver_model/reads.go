package main

import (
	"context"
	"fmt"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

// runRead picks a read target — usually a known account, sometimes one the model
// holds no state for — issues a linearizable GetAccount, and validates the result
// (the picked asset's volumes and the account's whole metadata map) against the
// model (see validateAccountRead).
func runRead(src *generation.Source, ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	state := c.modelState
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	// Picking runs lock-free on the snapshot; registering the read first only
	// holds the drain gate a little longer, never less.
	ledger, addr, asset, absentAccount, absentLedger, ok := pickReadTarget(src, state, liveLedgerNames(state, c.ledgerNamesSnapshot()))
	if !ok {
		return
	}

	// Both probes flow through the same validateAccountRead: the model models an
	// absent ledger as an empty ledger and an absent account as an empty account,
	// so the server must report empty (NotFound), and any cell or metadata no
	// candidate base explains is a finding.
	switch {
	case absentLedger:
		// Coverage: probing a ledger outside the fleet — the account cannot exist.
		assert.Reachable("singleton_driver_model: GetAccount probing an absent ledger", internal.Details{"ledger": ledger})
	case absentAccount:
		// Coverage: probing the negative space pickCell can't reach — an account the
		// model holds no state for, in a known ledger.
		assert.Reachable("singleton_driver_model: GetAccount probing a model-absent account", internal.Details{"ledger": ledger})
	}

	// Be explicit about consistency so the test still validates the
	// property it cares about if the server-side default ever changes.
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")

	// Colour collapse is a result-stage fold: every colour of an asset is summed
	// into the uncolored bucket.
	collapsed := oneIn(src, 3)

	responseFrontier := c.beginResponseFrontier()
	acct, err := client.GetAccount(readCtx, &servicepb.GetAccountRequest{
		Ledger:         ledger,
		Address:        addr,
		CollapseColors: collapsed,
	})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned. Captured before validation so later
	// dispatches aren't folded into this read's candidate states.
	maxTicket := responseFrontier()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		// NotFound = no entries server-side; validate as no volumes / no metadata.
		if status.Code(err) == codes.NotFound {
			c.validateAccountRead(maxTicket, ledger, addr, asset, nil, true, nil, false, collapsed)

			return
		}
		assert.Unreachable("singleton_driver_model: GetAccount returned unexpected error", internal.Details{
			"ledger":  ledger,
			"address": addr,
			"asset":   asset,
			"error":   err.Error(),
		})

		return
	}

	gotVols, wellFormed := accountVolumeSet(acct)
	c.validateAccountRead(maxTicket, ledger, addr, asset, gotVols, wellFormed, acct.GetMetadata(), true, collapsed)

	if collapsed && len(gotVols) > 0 {
		// Coverage: a colour-collapsed account read matched the folded model.
		assert.Reachable("singleton_driver_model: colour-collapsed account read validated", internal.Details{"ledger": ledger})
	}
}

// isShutdownError reports whether err is a context cancellation/deadline — what
// in-flight Apply/GetAccount calls return once MODEL_MAX_SECONDS expires (or the
// parent context is cancelled). It's a clean shutdown, not a server rejection to
// validate against, so callers drop the observation. (status.Code(nil) == OK.)
func isShutdownError(err error) bool {
	switch status.Code(err) {
	case codes.Canceled, codes.DeadlineExceeded:
		return true
	default:
		return false
	}
}

// percentChance compares a callback-scoped Hegel integer draw with pct.
// pct/100 is a nominal weight; Hegel controls the actual branch frequency.
func percentChance(src *generation.Source, pct uint64) bool {
	return src.Uint64Range(0, 100-1) < pct
}

// pickReadTarget chooses what GetAccount reads back. Most reads hit a known
// account (pickCell); nominally 5% probe an account the model has no state for in a known
// ledger (pickAbsentAccount, absentAccount=true); nominally 2% probe any account in a
// ledger outside the fleet (absentLedger=true). Both probes close blind spots
// pickCell can't reach — it only ever targets accounts the model already holds,
// so GetAccount is otherwise structurally blind to server state the model lacks
// (a retained cell, or an account in a ledger the model never created). Falls
// back to pickCell when no absent account is found. Runs lock-free on a state
// snapshot. A choice, not an expectation — see the package doc on generator and
// validator roles.
func pickReadTarget(src *generation.Source, g oracle.GlobalState, ledgers []string) (ledger, addr, asset string, absentAccount, absentLedger, ok bool) {
	if len(ledgers) > 0 && percentChance(src, 2) {
		return absentLedgerName(ledgers), poolAddress(src), generation.Choice(src, assets), false, true, true
	}

	if percentChance(src, 5) {
		if ledger, addr, asset, ok = pickAbsentAccount(src, g, ledgers); ok {
			return ledger, addr, asset, true, false, true
		}
	}

	ledger, addr, asset, ok = pickCell(src, g, ledgers)

	return ledger, addr, asset, false, false, ok
}

// pickAbsentAccount returns a (ledger, address, asset) the model holds no volume
// cell or metadata for — a pool-space address (t-N:M) the workload could create
// but has not touched. It reads a random workload asset so a server cell in any
// asset can be caught. ok=false when no ledger exists or no absent address turned
// up in a few tries (the pool space is far larger than what is touched, so this is
// rare). Runs lock-free on a state snapshot.
func pickAbsentAccount(src *generation.Source, g oracle.GlobalState, ledgers []string) (ledger, addr, asset string, ok bool) {
	if len(ledgers) == 0 {
		return "", "", "", false
	}

	ledger = generation.Choice(src, ledgers)
	ls := g.Ledger(ledger)
	for range 8 {
		cand := poolAddress(src)
		if !modelKnowsAccount(ls, cand) {
			return ledger, cand, generation.Choice(src, assets), true
		}
	}

	return "", "", "", false
}

// modelKnowsAccount reports whether ls holds any volume cell or metadata for
// addr — an O(log n) seek to the start of addr's key range in each table (an
// empty asset/key sorts before any real one, so the first entry at or after
// the probe shares addr's address iff the account has state).
func modelKnowsAccount(ls oracle.LedgerState, addr string) bool {
	for k := range ls.Volumes().From(oracle.VolumeKey{Address: addr}) {
		if k.Address == addr {
			return true
		}

		break // A later volume address does not rule out metadata on addr.
	}
	for k := range ls.Metadata().From(oracle.MetaKey{Address: addr}) {
		return k.Address == addr
	}

	return false
}

// pickCell returns a random readable account as (ledger, address, asset), or
// ok=false if the model holds nothing readable. It seeks a random pool-space
// probe into a random ledger's tables — O(log n) per pick, no table walk.
// nominally 1/4 of picks target a metadata-bearing address with an empty asset, so a
// metadata-only account is still reachable — the read validates that account's
// whole metadata map regardless of the asset. Ledgers are tried round-robin
// from a random start so an empty one doesn't blind the pick.
func pickCell(src *generation.Source, g oracle.GlobalState, ledgers []string) (ledger, addr, asset string, ok bool) {
	if len(ledgers) == 0 {
		return "", "", "", false
	}

	start := int(src.Uint64Range(0, (uint64(len(ledgers)))-1))
	metaPick := percentChance(src, 25)

	for i := range ledgers {
		name := ledgers[(start+i)%len(ledgers)]
		ls := g.Ledger(name)

		if metaPick {
			if mk, _, found := pickAtOrAfter(ls.Metadata(), oracle.MetaKey{Address: poolAddress(src), Key: metaKey(src)}); found {
				return name, mk.Address, "", true
			}
		}

		if k, _, found := pickAtOrAfter(ls.Volumes(), oracle.VolumeKey{Address: poolAddress(src), Asset: generation.Choice(src, assets)}); found {
			return name, k.Address, k.Asset, true
		}

		// No volumes in this ledger: a metadata-only account still qualifies.
		if mk, _, found := pickAtOrAfter(ls.Metadata(), oracle.MetaKey{Address: poolAddress(src), Key: metaKey(src)}); found {
			return name, mk.Address, "", true
		}
	}

	return "", "", "", false
}

// accountVolumeSet extracts the account's full volume set as (asset, color) ->
// volumes, so validation covers every returned cell — a ghost row under any
// bucket is caught, not just the probed one. ok=false marks a response shape no
// model state explains — a repeated bucket, or an unparseable amount — and the
// caller's validation fails it outright rather than mistaking it for an empty
// reading.
func accountVolumeSet(acct *commonpb.Account) (map[assetColor]oracle.VolumePair, bool) {
	if acct == nil {
		return nil, true
	}

	out := make(map[assetColor]oracle.VolumePair, len(acct.GetVolumes()))
	for _, entry := range acct.GetVolumes() {
		key := assetColor{Asset: entry.GetAsset(), Color: entry.GetColor()}
		if _, dup := out[key]; dup {
			return nil, false
		}

		var vp oracle.VolumePair
		vols := entry.GetVolumes()
		if vols == nil || vols.GetInput() == nil || vols.GetOutput() == nil {
			return nil, false
		}
		if vp.Input.SetFromDecimal(vols.GetInput().DecimalString()) != nil ||
			vp.Output.SetFromDecimal(vols.GetOutput().DecimalString()) != nil {
			return nil, false
		}

		out[key] = vp
	}

	return out, true
}

// absentLedgerName returns a ledger name outside the fleet — the negative-space
// target for a ledger-scoped read. No workload ledger is ever named absent-…,
// so the name is guaranteed absent; the server answers NotFound for it, a pure
// read that creates nothing.
func absentLedgerName(ledgers []string) string {
	known := make(map[string]bool, len(ledgers))
	for _, l := range ledgers {
		known[l] = true
	}

	for {
		cand := internal.PrefixModel.WithSuffix(fmt.Sprintf("absent-%016x%016x", internal.Rand().Uint64(), internal.Rand().Uint64()))
		if !known[cand] {
			return cand
		}
	}
}

// pickLedgerReadTarget chooses a ledger for a ledger-scoped read: usually a known
// fleet ledger, else an absent one (absent=true) with nominal weight absentPct/100. The
// absent probe closes the blind spot a known-only pick leaves — a ledger-scoped
// read can never otherwise detect the server serving a ledger the model never
// created. An absent ledger must answer NotFound; a served snapshot is a finding.
func pickLedgerReadTarget(src *generation.Source, ledgers []string, absentPct uint64) (ledger string, absent bool) {
	if len(ledgers) == 0 || percentChance(src, absentPct) {
		return absentLedgerName(ledgers), true
	}

	return generation.Choice(src, ledgers), false
}

// runLedgerRead issues a linearizable GetLedger — usually on a fleet ledger,
// sometimes on an absent one — and checks the result against the model: a fleet
// ledger's whole snapshot (account types and ledger metadata, see
// validateLedgerRead), or an absent ledger's mandatory NotFound.
func runLedgerRead(src *generation.Source, ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(src, c.liveLedgerNamesSnapshot(), 2)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	responseFrontier := c.beginResponseFrontier()
	info, err := client.GetLedger(readCtx, &servicepb.GetLedgerRequest{Ledger: ledger})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := responseFrontier()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if status.Code(err) == codes.NotFound {
			if absent {
				assert.Reachable("singleton_driver_model: GetLedger on an absent ledger returned NotFound", internal.Details{"ledger": ledger})
			}
			c.validateLedgerNotFound(maxTicket, ledger, "GetLedger")

			return
		}
		assert.Unreachable("singleton_driver_model: GetLedger returned unexpected error", internal.Details{
			"ledger": ledger,
			"absent": absent,
			"error":  err.Error(),
		})

		return
	}

	if absent {
		assert.Unreachable("singleton_driver_model: GetLedger served a ledger outside the fleet", internal.Details{"ledger": ledger})

		return
	}

	c.validateLedgerRead(maxTicket, ledger, info.GetAccountTypes(), info.GetMetadata(), info.GetDefaultEnforcementMode())
}

// pickTransactionID picks a ledger and a transaction id to read back. Usually a
// fleet ledger, probing up to a small slack past the committed frontier so the id
// may land on a committed transaction, an in-flight one, or an unassigned id (a
// legal NotFound). Nominally 2% target a ledger outside the fleet (absentLedger=true),
// where no transaction can exist and any id must resolve NotFound. ok=false only
// before any ledger exists.
func pickTransactionID(src *generation.Source, g oracle.GlobalState, ledgers []string) (ledger string, id uint64, absentLedger, ok bool) {
	if len(ledgers) == 0 {
		return "", 0, false, false
	}

	if percentChance(src, 2) {
		return absentLedgerName(ledgers), 1 + src.Uint64Range(0, 64-1), true, true
	}

	ledger = generation.Choice(src, ledgers)
	const slack = 8
	frontier := uint64(g.Ledger(ledger).Txs().Len())
	id = 1 + src.Uint64Range(0, (frontier+slack)-1)

	return ledger, id, false, true
}

// runTransactionRead issues a linearizable GetTransaction on a probed id and
// checks the observation — a returned transaction, or NotFound — against the
// model (see validateTransactionRead). This is the only path that reads
// accumulated transaction metadata back.
func runTransactionRead(src *generation.Source, ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	state := c.modelState
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	ledger, id, absentLedger, ok := pickTransactionID(src, state, liveLedgerNames(state, c.ledgerNamesSnapshot()))
	if !ok {
		return
	}

	if absentLedger {
		// Coverage: probing a ledger outside the fleet — no transaction can exist,
		// so the server must answer NotFound (validateTransactionRead treats the
		// absent ledger as empty); a returned transaction is a finding.
		assert.Reachable("singleton_driver_model: GetTransaction probing an absent ledger", internal.Details{"ledger": ledger})
	}

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	responseFrontier := c.beginResponseFrontier()
	resp, err := client.GetTransaction(readCtx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: id})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := responseFrontier()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		// NotFound is a legal outcome for an id not committed in the actual
		// serialization — validate it like a returned transaction, not a finding.
		if status.Code(err) == codes.NotFound {
			c.validateTransactionRead(maxTicket, ledger, id, nil, false)

			return
		}
		assert.Unreachable("singleton_driver_model: GetTransaction returned unexpected error", internal.Details{
			"ledger": ledger,
			"id":     id,
			"error":  err.Error(),
		})

		return
	}

	c.validateTransactionRead(maxTicket, ledger, id, resp.GetTransaction(), true)
}

// runSchemaRead issues a GetMetadataSchemaStatus and checks the declared metadata
// field types (account / transaction / ledger) against the model (see
// validateSchemaRead) — the read-back that verifies the declared-schema
// projection, not just the per-op SetMetadataFieldType echo.
func runSchemaRead(src *generation.Source, ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(src, c.liveLedgerNamesSnapshot(), 3)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	responseFrontier := c.beginResponseFrontier()
	resp, err := client.GetMetadataSchemaStatus(readCtx, &servicepb.GetMetadataSchemaStatusRequest{Ledger: ledger})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := responseFrontier()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if status.Code(err) == codes.NotFound {
			if absent {
				assert.Reachable("singleton_driver_model: GetMetadataSchemaStatus on an absent ledger returned NotFound", internal.Details{"ledger": ledger})
			}
			c.validateLedgerNotFound(maxTicket, ledger, "GetMetadataSchemaStatus")

			return
		}
		assert.Unreachable("singleton_driver_model: GetMetadataSchemaStatus returned unexpected error", internal.Details{
			"ledger": ledger,
			"absent": absent,
			"error":  err.Error(),
		})

		return
	}

	if absent {
		assert.Unreachable("singleton_driver_model: GetMetadataSchemaStatus served a ledger outside the fleet", internal.Details{"ledger": ledger})

		return
	}

	c.validateSchemaRead(maxTicket, ledger, resp.GetAccountFields(), resp.GetTransactionFields(), resp.GetLedgerFields())
}
