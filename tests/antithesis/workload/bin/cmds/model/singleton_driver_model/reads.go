package main

import (
	"context"
	"fmt"
	"slices"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/holiman/uint256"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// runRead picks a read target — usually a known account, sometimes one the model
// holds no state for — issues a linearizable GetAccount, and validates the result
// (the picked asset's volumes and the account's whole metadata map) against the
// model (see validateAccountRead).
func runRead(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	ledger, addr, asset, absentAccount, absentLedger, ok := pickReadTarget(c.modelState, c.ledgerNames)
	if !ok {
		c.mu.Unlock()
		return
	}
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

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

	acct, err := client.GetAccount(readCtx, &servicepb.GetAccountRequest{
		Ledger:  ledger,
		Address: addr,
	})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned. Captured before validation so later
	// dispatches aren't folded into this read's candidate states.
	maxTicket := c.ticketSeq.Load()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		// NotFound = no entries server-side; validate as zero volumes / no metadata.
		if status.Code(err) == codes.NotFound {
			c.validateAccountRead(maxTicket, ledger, addr, asset, uint256.Int{}, uint256.Int{}, false, nil)
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

	gotIn, gotOut, found := accountAssetVolumes(acct, asset)
	c.validateAccountRead(maxTicket, ledger, addr, asset, gotIn, gotOut, found, acct.GetMetadata())
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

// percentChance reports true with probability pct/100, drawing from the
// Antithesis-controlled source so the fuzzer steers how often the branch is taken.
func percentChance(pct uint64) bool {
	return internal.Rand().Uint64()%100 < pct
}

// pickReadTarget chooses what GetAccount reads back. Most reads hit a known
// account (pickCell); ~5% probe an account the model has no state for in a known
// ledger (pickAbsentAccount, absentAccount=true); ~2% probe any account in a
// ledger outside the fleet (absentLedger=true). Both probes close blind spots
// pickCell can't reach — it only ever targets accounts the model already holds,
// so GetAccount is otherwise structurally blind to server state the model lacks
// (a retained cell, or an account in a ledger the model never created). Falls
// back to pickCell when no absent account is found. Caller holds c.mu.
func pickReadTarget(g oracle.GlobalState, ledgers []string) (ledger, addr, asset string, absentAccount, absentLedger, ok bool) {
	if len(ledgers) > 0 && percentChance(2) {
		return absentLedgerName(ledgers), poolAddress(), random.RandomChoice(assets), false, true, true
	}

	if percentChance(5) {
		if ledger, addr, asset, ok = pickAbsentAccount(g, ledgers); ok {
			return ledger, addr, asset, true, false, true
		}
	}

	ledger, addr, asset, ok = pickCell(g)

	return ledger, addr, asset, false, false, ok
}

// pickAbsentAccount returns a (ledger, address, asset) the model holds no volume
// cell or metadata for — a pool-space address (t-N:M) the workload could create
// but has not touched. It reads a random workload asset so a server cell in any
// asset can be caught. ok=false when no ledger exists or no absent address turned
// up in a few tries (the pool space is far larger than what is touched, so this is
// rare). Caller holds c.mu.
func pickAbsentAccount(g oracle.GlobalState, ledgers []string) (ledger, addr, asset string, ok bool) {
	if len(ledgers) == 0 {
		return "", "", "", false
	}

	ledger = random.RandomChoice(ledgers)
	ls := g.Ledger(ledger)
	for tries := 0; tries < 8; tries++ {
		cand := poolAddress()
		if !modelKnowsAccount(ls, cand) {
			return ledger, cand, random.RandomChoice(assets), true
		}
	}

	return "", "", "", false
}

// modelKnowsAccount reports whether ls holds any volume cell or metadata for addr.
func modelKnowsAccount(ls oracle.LedgerState, addr string) bool {
	for k := range ls.Volumes() {
		if k.Address == addr {
			return true
		}
	}
	for k := range ls.Metadata() {
		if k.Address == addr {
			return true
		}
	}

	return false
}

// pickCell returns a random readable account across all ledgers as
// (ledger, address, asset), or ok=false if there are none. Each volume cell is a
// candidate (carrying its asset); each metadata-bearing address is also a
// candidate with an empty asset, so a metadata-only account is still reachable —
// the read validates that account's metadata regardless of the asset.
func pickCell(g oracle.GlobalState) (ledger, addr, asset string, ok bool) {
	type cellRef struct {
		ledger string
		key    oracle.VolumeKey
	}

	var cells []cellRef
	for name, ls := range g.Ledgers() {
		for k := range ls.Volumes() {
			cells = append(cells, cellRef{ledger: name, key: k})
		}

		metaAddrs := map[string]bool{}
		for mk := range ls.Metadata() {
			metaAddrs[mk.Address] = true
		}
		for a := range metaAddrs {
			cells = append(cells, cellRef{ledger: name, key: oracle.VolumeKey{Address: a}})
		}
	}

	if len(cells) == 0 {
		return "", "", "", false
	}

	slices.SortFunc(cells, func(a, b cellRef) int {
		if a.ledger != b.ledger {
			if a.ledger < b.ledger {
				return -1
			}
			return 1
		}
		return oracle.CompareVolumeKey(a.key, b.key)
	})

	c := random.RandomChoice(cells)

	return c.ledger, c.key.Address, c.key.Asset, true
}

// accountAssetVolumes extracts (input, output) for one asset from a GetAccount
// response. The workload only ever exercises uncolored postings, so we look
// up the uncolored bucket (color="") explicitly — colored buckets are out of
// scope for this driver model. found=false when the bucket is missing.
func accountAssetVolumes(acct *commonpb.Account, asset string) (in, out uint256.Int, found bool) {
	if acct == nil {
		return in, out, false
	}

	v := acct.FindVolume(asset, "")
	if v == nil {
		return in, out, false
	}

	if err := in.SetFromDecimal(v.GetInput()); err != nil {
		in.Clear()
	}

	if err := out.SetFromDecimal(v.GetOutput()); err != nil {
		out.Clear()
	}

	return in, out, true
}

// absentLedgerName returns a ledger name outside the fixed fleet — the
// negative-space target for a ledger-scoped read. The fleet is created at setup
// and never grows, so any name not in ledgers is guaranteed absent; the server
// answers NotFound for it, a pure read that creates nothing.
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
// fleet ledger, else an absent one (absent=true) with probability absentPct. The
// absent probe closes the blind spot a known-only pick leaves — a ledger-scoped
// read can never otherwise detect the server serving a ledger the model never
// created. An absent ledger must answer NotFound; a served snapshot is a finding.
func pickLedgerReadTarget(ledgers []string, absentPct uint64) (ledger string, absent bool) {
	if percentChance(absentPct) {
		return absentLedgerName(ledgers), true
	}

	return random.RandomChoice(ledgers), false
}

// runLedgerRead issues a linearizable GetLedger — usually on a fleet ledger,
// sometimes on an absent one — and checks the result against the model: a fleet
// ledger's whole snapshot (account types and ledger metadata, see
// validateLedgerRead), or an absent ledger's mandatory NotFound.
func runLedgerRead(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(c.ledgerNames, 2)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	info, err := client.GetLedger(readCtx, &servicepb.GetLedgerRequest{Ledger: ledger})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if absent && status.Code(err) == codes.NotFound {
			// Coverage: a ledger outside the fleet must resolve NotFound.
			assert.Reachable("singleton_driver_model: GetLedger on an absent ledger returned NotFound", internal.Details{"ledger": ledger})
			return
		}
		// A fleet ledger is created at setup and never deleted, so a definitive
		// error on it — NotFound, Internal — is a real finding; so is any
		// non-NotFound definitive error on an absent ledger.
		assert.Unreachable("singleton_driver_model: GetLedger returned unexpected error", internal.Details{
			"ledger": ledger,
			"absent": absent,
			"error":  err.Error(),
		})
		return
	}

	if absent {
		// The fleet never grows, so a snapshot for a name outside it is a ledger the
		// server holds but the model never created.
		assert.Unreachable("singleton_driver_model: GetLedger served a ledger outside the fleet", internal.Details{"ledger": ledger})
		return
	}

	c.validateLedgerRead(maxTicket, ledger, info.GetAccountTypes(), info.GetMetadata())
}

// pickTransactionID picks a ledger and a transaction id to read back. Usually a
// fleet ledger, probing up to a small slack past the committed frontier so the id
// may land on a committed transaction, an in-flight one, or an unassigned id (a
// legal NotFound). ~2% target a ledger outside the fleet (absentLedger=true),
// where no transaction can exist and any id must resolve NotFound. ok=false only
// before any ledger exists.
func pickTransactionID(g oracle.GlobalState, ledgers []string) (ledger string, id uint64, absentLedger, ok bool) {
	if len(ledgers) == 0 {
		return "", 0, false, false
	}

	if percentChance(2) {
		return absentLedgerName(ledgers), 1 + internal.Rand().Uint64()%64, true, true
	}

	ledger = random.RandomChoice(ledgers)
	const slack = 8
	frontier := uint64(len(g.Ledger(ledger).Txs()))
	id = 1 + internal.Rand().Uint64()%(frontier+slack)

	return ledger, id, false, true
}

// runTransactionRead issues a linearizable GetTransaction on a probed id and
// checks the observation — a returned transaction, or NotFound — against the
// model (see validateTransactionRead). This is the only path that reads
// accumulated transaction metadata back.
func runTransactionRead(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	c.mu.Lock()
	ledger, id, absentLedger, ok := pickTransactionID(c.modelState, c.ledgerNames)
	if !ok {
		c.mu.Unlock()
		return
	}
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	if absentLedger {
		// Coverage: probing a ledger outside the fleet — no transaction can exist,
		// so the server must answer NotFound (validateTransactionRead treats the
		// absent ledger as empty); a returned transaction is a finding.
		assert.Reachable("singleton_driver_model: GetTransaction probing an absent ledger", internal.Details{"ledger": ledger})
	}

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	resp, err := client.GetTransaction(readCtx, &servicepb.GetTransactionRequest{Ledger: ledger, TransactionId: id})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()
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
func runSchemaRead(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger, absent := pickLedgerReadTarget(c.ledgerNames, 3)

	c.mu.Lock()
	readID := c.registerRead()
	c.mu.Unlock()
	defer c.finishRead(readID)

	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	resp, err := client.GetMetadataSchemaStatus(readCtx, &servicepb.GetMetadataSchemaStatusRequest{Ledger: ledger})
	// High-water at the read's response: only bulks dispatched by now could be
	// reflected in what the server returned.
	maxTicket := c.ticketSeq.Load()
	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if absent && status.Code(err) == codes.NotFound {
			// Coverage: a schema read of a ledger outside the fleet must resolve NotFound.
			assert.Reachable("singleton_driver_model: GetMetadataSchemaStatus on an absent ledger returned NotFound", internal.Details{"ledger": ledger})
			return
		}
		// A fleet ledger is created at setup and never deleted, so a definitive
		// error on it is a real finding; so is any non-NotFound definitive error on
		// an absent ledger.
		assert.Unreachable("singleton_driver_model: GetMetadataSchemaStatus returned unexpected error", internal.Details{
			"ledger": ledger,
			"absent": absent,
			"error":  err.Error(),
		})
		return
	}

	if absent {
		// The fleet never grows, so a schema for a name outside it is a ledger the
		// server holds but the model never created.
		assert.Unreachable("singleton_driver_model: GetMetadataSchemaStatus served a ledger outside the fleet", internal.Details{"ledger": ledger})
		return
	}

	c.validateSchemaRead(maxTicket, ledger, resp.GetAccountFields(), resp.GetTransactionFields(), resp.GetLedgerFields())
}
