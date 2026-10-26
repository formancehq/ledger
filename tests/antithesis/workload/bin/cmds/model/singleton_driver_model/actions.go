package main

import (
	"fmt"
	"math/big"
	"slices"
	"strconv"
	"strings"

	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/pkg/actions"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

// applyRequest renders a bulk into a sendable ApplyRequest. Idempotency is per
// ApplyBatch, so the whole batch carries one key. A replayable original (and its
// later replay) carries a tracked key the model froze; every other bulk gets a
// fresh ephemeral key — untracked by the model, but still making the client's
// internal retries safe (an ambiguous UNAVAILABLE replays the committed batch
// instead of re-applying it).
func applyRequest(b oracle.Bulk) *servicepb.ApplyRequest {
	key := b.IdempotencyKey
	if key == "" {
		key = idempotencyKey()
	}

	return servicepb.UnsignedApplyRequest(key, b.Requests...)
}

// indexPool builds a 0..n-1 index slice for a 1-in-n RandomChoice roll, derived
// from the const so the odds and the pool stay in lockstep (see typePool).
func indexPool(n int) []uint8 {
	out := make([]uint8, n)
	for i := range out {
		out[i] = uint8(i)
	}

	return out
}

var (
	replayTrackPool    = indexPool(replayTrackOneIn)
	replayConflictPool = indexPool(replayConflictOneIn)
)

// rollReplayTrack reports whether to stamp a bulk as a replayable original.
func rollReplayTrack(src *generation.Source) bool {
	return generation.Choice(src, replayTrackPool) == 0
}

// rollConflict reports whether to reuse a committed key on a different body.
func rollConflict(src *generation.Source) bool {
	return generation.Choice(src, replayConflictPool) == 0
}

// Pool: every type name and every tx address uses a "t-N" prefix where
// N comes from typePool. The pool is small on purpose — collisions
// (race-add, race-remove, untyped tx) are the test's chaos surface.

// "t-3"-style name.
func poolName(src *generation.Source) string {
	return fmt.Sprintf("t-%d", generation.Choice(src, typePool))
}

// "t-N:i" — same prefix space as type names. numIDsPerPrefix is
// small enough that re-targeting the same cell is frequent.
func poolAddress(src *generation.Source) string {
	return fmt.Sprintf("%s:%d", poolName(src), src.Uint64Range(0, numIDsPerPrefix-1))
}

// randomColor uses explicit equal-probability decisions for the geometric
// distribution: "" half the time, "A" a quarter, "B" an eighth, on down to
// a 65th bucket at 2^-64. The head keeps same-(account,
// asset) color collisions frequent, which is what exposes a leak between
// buckets; the tail keeps producing buckets touched once and never again,
// where a leak has no re-touch to give it away. Drawn at the point of decision
// so Hegel controls it. A biased uint64 followed by trailing-zero counting
// would distort the head frequencies and overrepresent extreme buckets.
func randomColor(src *generation.Source) string {
	depth := 0
	for depth < 64 && src.Bool() {
		depth++
	}

	return colorName(depth)
}

// colorName encodes k as bijective base-26 letters — 0 is the uncolored
// bucket, then A..Z, AA..ZZ — satisfying the ^[A-Z]*$ charset the admission
// boundary enforces.
func colorName(k int) string {
	var out []byte
	for k > 0 {
		k--
		out = append(out, byte('A'+k%26))
		k /= 26
	}

	slices.Reverse(out)

	return string(out)
}

// pickAtOrAfter returns the first entry at or after probe, wrapping to the
// smallest key — an O(log n) near-uniform random pick when probe is drawn
// uniformly over the key space, in place of collecting every entry.
func pickAtOrAfter[K, V any](m oracle.Map[K, V], probe K) (K, V, bool) {
	for k, v := range m.From(probe) {
		return k, v, true
	}
	for k, v := range m.All() {
		return k, v, true
	}

	var (
		k K
		v V
	)

	return k, v, false
}

// sourceAddress picks a debit source without filtering on balance: "world"
// (overdraftable) about half the time to keep accounts funded, otherwise any
// pool address — most of which are underfunded, so a non-forced debit exercises
// the INSUFFICIENT_FUNDS floor.
func sourceAddress(src *generation.Source) string {
	if generation.Choice(src, []uint8{0, 1}) == 0 {
		return "world"
	}

	return poolAddress(src)
}

// generateBulk plans the next bulk. Most bulks target a single ledger;
// occasionally a bulk spreads its requests across a few, exercising the
// server's atomic-across-ledgers semantics. Runs lock-free on a state
// snapshot (a published GlobalState is never mutated — Apply forks first).
func generateBulk(src *generation.Source, g oracle.GlobalState, ledgers []string, newLedger string, liveTarget int) oracle.Bulk {
	active := activeLedgers(g, ledgers)
	if req := generateLifecycle(src, g, ledgers, newLedger, liveTarget); req != nil {
		return oracle.Bulk{Requests: []*servicepb.Request{req}}
	}
	if len(active) == 0 {
		return oracle.Bulk{}
	}
	picks := pickLedgers(src, active)

	// Whole-bulk transient shapes fund and drain the same cell, so they only
	// make sense single-ledger. Gate them by the same back-pressure as
	// generateTransaction so their unreferenced fund/drain records don't grow the
	// log past the cap.
	if len(picks) == 1 {
		ledger := picks[0]
		ls := g.Ledger(ledger)
		if rollTransaction(src, ls) {
			switch generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}) {
			case 0:
				if reqs := generateTransientBalancedBulk(src, ledger, ls); reqs != nil {
					return oracle.Bulk{Requests: reqs}
				}
			case 1:
				if reqs := generateTransientUnbalancedBulk(src, ledger, ls); reqs != nil {
					return oracle.Bulk{Requests: reqs}
				}
			}
		}
	}

	// An index create/drop is its own single-request bulk (single-ledger, since
	// the index is ledger-scoped). Emitting it alone keeps the model's index
	// lifecycle a clean sequence of committed CreateIndex/DropIndex orders.
	if len(picks) == 1 && rollIndexOp(src, g.Ledger(picks[0])) {
		if req := generateIndexOp(src, g, picks[0]); req != nil {
			return oracle.Bulk{Requests: []*servicepb.Request{req}}
		}
	}

	// A prepared-query registry op is likewise its own single-request bulk: the
	// registry is ledger-scoped, and keeping the lifecycle a clean sequence of
	// committed create/update/delete orders is what lets an execution read
	// validate against a candidate base's stored definition.
	if len(picks) == 1 && rollPreparedQueryOp(src) {
		if req := generatePreparedQueryOp(src, g, picks[0]); req != nil {
			return oracle.Bulk{Requests: []*servicepb.Request{req}}
		}
	}

	size := bulkSize(src)
	requests := make([]*servicepb.Request, 0, size)
	appendRequest := func(ls oracle.LedgerState, req *servicepb.Request) {
		requests = append(requests, maybeAddSkippableReason(src, ls, req))
	}

	for range size {
		ledger := generation.Choice(src, picks)
		ls := g.Ledger(ledger)

		if generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}) == 0 {
			appendRequest(ls, generateEnforcementMode(src, ledger))

			continue
		}
		if rollChartOp(src) {
			if req := generateChartOp(src, ledger); req != nil {
				appendRequest(ls, req)

				continue
			}
		}

		if rollSchemaOp(src) {
			if req := generateSchemaOp(src, ledger, ls); req != nil {
				appendRequest(ls, req)

				continue
			}
		}

		if rollMetadataOp(src) {
			if req := generateMetadataOp(src, ledger, ls); req != nil {
				appendRequest(ls, req)

				continue
			}
		}

		if rollRevert(src) {
			if req := generateRevert(src, ledger, ls); req != nil {
				appendRequest(ls, req)

				continue
			}
		}

		if rollTransaction(src, ls) {
			if req := generateTransaction(src, ledger, ls); req != nil {
				appendRequest(ls, req)

				continue
			}
		}

		// Transaction back-pressure kicked in: emit a metadata op instead so the
		// slot stays productive and the workload exercises existing state rather
		// than creating ever more transactions as the ledger fills up.
		if req := generateMetadataOp(src, ledger, ls); req != nil {
			appendRequest(ls, req)
		}
	}

	// The "and continued" probes need an order executed after the skip, and most
	// bulks hold a single order. Half the trailing opt-ins get a successor that
	// always commits; the rest stay trailing, where the skip proves only that it
	// did not fail the bulk.
	if n := len(requests); n > 0 && len(requests[n-1].GetApply().GetSkippableReasons()) > 0 &&
		generation.Choice(src, []uint8{0, 1}) == 0 {
		requests = append(requests, generateAddMetadata(src, requests[n-1].GetApply().GetLedger()))
	}

	return oracle.Bulk{Requests: requests}
}

func activeLedgers(g oracle.GlobalState, ledgers []string) []string {
	live := liveLedgerNames(g, ledgers)
	out := make([]string, 0, len(live))
	for _, name := range live {
		if lc, ok := g.Lifecycle(name); ok && lc.Mode != commonpb.LedgerMode_LEDGER_MODE_MIRROR {
			out = append(out, name)
		}
	}

	return out
}

// generateLifecycle mixes administrative transitions into the same concurrent
// stream as business writes. Creation is biased when deletions shrink the live
// pool, providing the same bounded-state back-pressure as transaction creation.
func generateLifecycle(src *generation.Source, g oracle.GlobalState, ledgers []string, newLedger string, liveTarget int) *servicepb.Request {
	live, deleted := partitionLifecycleLedgers(g, ledgers)
	if len(deleted) > 0 && generation.Choice(src, indexPool(32)) == 0 {
		return actions.CreateLedgerAction(generation.Choice(src, deleted), nil)
	}
	if len(live) < liveTarget {
		if generation.Choice(src, []uint8{0, 1, 2, 3}) == 0 {
			return &servicepb.Request{Type: &servicepb.Request_CreateLedger{CreateLedger: &servicepb.CreateLedgerRequest{
				Name: newLedger, Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR,
				Metadata:     randomMetaMap(src),
				MirrorSource: &commonpb.MirrorSourceConfig{LedgerName: "unused"},
			}}}
		}

		request := actions.CreateLedgerAction(newLedger, nil)
		request.GetCreateLedger().Metadata = randomMetaMap(src)

		return request
	}
	if generation.Choice(src, indexPool(24)) != 0 {
		return nil
	}

	switch generation.Choice(src, []uint8{0, 1, 2, 3}) {
	case 0:
		if len(live) > 0 {
			return actions.DeleteLedgerAction(generation.Choice(src, live))
		}

		return nil
	case 1:
		mirrors := make([]string, 0, len(live))
		for _, name := range live {
			if lc, ok := g.Lifecycle(name); ok && lc.Mode == commonpb.LedgerMode_LEDGER_MODE_MIRROR {
				mirrors = append(mirrors, name)
			}
		}
		if len(mirrors) == 0 {
			return nil
		}

		return &servicepb.Request{Type: &servicepb.Request_PromoteLedger{PromoteLedger: &servicepb.PromoteLedgerRequest{Ledger: generation.Choice(src, mirrors)}}}
	default:
		// Enables stop every business worker for the recovery window, so generate
		// them much less often than disables. An active maintenance window still
		// gets the full nominal toggle weight.
		if !g.MaintenanceMode() && generation.Choice(src, indexPool(8)) != 0 {
			return nil
		}

		return actions.SetMaintenanceModeAction(!g.MaintenanceMode())
	}
}

// rollTransaction reports whether to create a new transaction, tapering with the
// ledger's full transaction-log size (see the txEmit* knobs) so the log — every
// entry of which candidateBases clones and hashes on each fold — doesn't grow
// without bound. Counting only referenced transactions would miss the
// unreferenced records (reverts, drains, transient helpers) that also accumulate.
func rollTransaction(src *generation.Source, ls oracle.LedgerState) bool {
	switch n := ls.Txs().Len(); {
	case n < txEmitFull:
		return true
	case n < txEmitTaper:
		return generation.Choice(src, []uint8{0, 1}) == 0
	case n < txEmitStop:
		return generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5, 6, 7}) == 0
	default:
		return false
	}
}

// pickLedgers returns the ledger(s) a bulk's requests draw from: usually one,
// nominally 1-in-6 a distinct 2-3 so the bulk spans ledgers.
func pickLedgers(src *generation.Source, ledgers []string) []string {
	if len(ledgers) < 2 || generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5}) != 0 {
		return []string{generation.Choice(src, ledgers)}
	}

	n := 2
	if len(ledgers) >= 3 && generation.Choice(src, []uint8{0, 1}) == 0 {
		n = 3
	}

	return sampleDistinct(src, ledgers, n)
}

// sampleDistinct picks n distinct ledgers via the Antithesis RNG (rejection
// sampling; n <= len(ledgers) so it terminates).
func sampleDistinct(src *generation.Source, ledgers []string, n int) []string {
	seen := map[string]bool{}
	out := make([]string, 0, n)
	for len(out) < n {
		c := generation.Choice(src, ledgers)
		if !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}

	return out
}

// nominally 30%: a chart op fills this bulk slot, else a transaction.
func rollChartOp(src *generation.Source) bool {
	return generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) < 3
}

// nominally 1-in-48: a metadata-schema op (declare/remove a field type) fills this slot.
// Intended to allow a retyped index to see several readiness polls
// (indexPollInterval apart) before its next retype, so its window can close;
// successive draws can still chain retypes within a poll to exercise window
// extension. These are nominal weights, not timing or frequency guarantees.
func rollSchemaOp(src *generation.Source) bool {
	return src.Uint64Range(0, 48-1) == 0
}

// nominally 25%: a metadata op fills this bulk slot, when a chart/schema op didn't.
func rollMetadataOp(src *generation.Source) bool {
	return generation.Choice(src, []uint8{0, 1, 2, 3}) == 0
}

// nominally 1-in-6: revert a committed transaction in this slot.
func rollRevert(src *generation.Source) bool {
	return generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5}) == 0
}

// Picks Add vs Remove.
func generateChartOp(src *generation.Source, ledger string) *servicepb.Request {
	if generation.Choice(src, []uint8{0, 1}) == 0 {
		return generateAddAccountType(src, ledger)
	}

	return generateRemoveAccountType(src, ledger)
}

// Nominal pool weights: 5/8 single, 2/8 pair, 1/8 in {3,4,5}.
// Hegel integer bias can change observed frequencies.
func bulkSize(src *generation.Source) int {
	switch generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5, 6, 7}) {
	case 0, 1, 2, 3, 4:
		return 1
	case 5, 6:
		return 2
	default:
		return int(generation.Choice(src, []uint8{3, 4, 5}))
	}
}

// randomTransientType returns a random TRANSIENT type from ls's chart, or nil.
func randomTransientType(src *generation.Source, ls oracle.LedgerState) *oracle.TypeState {
	// Types().All() iterates in key order, so names is already sorted —
	// replayable / steerable via the Antithesis RNG.
	var names []string
	for name, t := range ls.Types().All() {
		if t.Persistence == commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT {
			names = append(names, name)
		}
	}

	if len(names) == 0 {
		return nil
	}

	t, _ := ls.Types().Get(generation.Choice(src, names))

	return &t
}

// --- Per-action generators ------------------------------------------------

// A chaos transaction of 1–4 postings between blindly-picked pool addresses
// (source may be "world"), amounts and force rolled at random so the server
// resolves typing, the balance floor, intra-transaction posting composition,
// and — with force — overdrafts; or, with nominal weight 1/4, a deliberate
// drain of an EPHEMERAL cell to exercise the purge sweep.
func generateTransaction(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	if generation.Choice(src, []uint8{0, 1, 2, 3}) == 0 {
		if req := generateDrainTransaction(src, ledger, ls); req != nil {
			return req
		}
	}

	// Keep the bounded CI run deterministic enough to exercise the reference-
	// conflict skip path even after lifecycle operations consume generation
	// slots. The ordinary malformed branch below still emits unskipped conflicts.
	if generation.Choice(src, indexPool(16)) == 0 {
		if req := duplicateReferenceTransaction(src, ledger, ls); req != nil {
			req.GetApply().SkippableReasons = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_TRANSACTION_REFERENCE_CONFLICT}

			return req
		}
	}

	// nominally 1/16: a deliberately-malformed transaction exercising a create rejection
	// branch (empty / duplicate-reference / volume-overflow). The server rejects
	// it and the oracle reproduces the reason, so validateFailure explains it.
	if generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}) == 0 {
		if req := generateRejectedTransaction(src, ledger, ls); req != nil {
			return req
		}
	}

	// Force (nominally 1/8) skips the balance floor, letting a non-world source overdraft.
	force := generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5, 6, 7}) == 0

	// 1–4 postings, each between blindly-picked accounts. Multiple postings
	// commit atomically and compose in order — an earlier posting can fund a
	// later one's source (the running balance floor is per-posting).
	n := 1 + int(generation.Choice(src, []uint8{0, 1, 2, 3}))
	postings := make([]*commonpb.Posting, n)
	for i := range postings {
		postings[i] = commonpb.NewColoredPosting(sourceAddress(src), poolAddress(src), assets[int(generation.Choice(src, []uint8{0, 1, 2}))], randomColor(src), internal.GeneratedBigInt(src))
	}

	// Every transaction gets a unique reference so it is targetable by later
	// transaction-metadata writes. nominally half also carry metadata at creation.
	payload := &servicepb.CreateTransactionPayload{
		Postings:  postings,
		Reference: txRef(),
		Force:     force,
	}

	if generation.Choice(src, []uint8{0, 1}) == 0 {
		payload.Metadata = randomMetaMap(src)
	}

	// nominally half also carry a user-supplied timestamp — stored verbatim and echoed on
	// reads (it does not feed the HLC / log order). Backdated to stay non-future.
	if generation.Choice(src, []uint8{0, 1}) == 0 {
		payload.Timestamp = &commonpb.Timestamp{Data: 1 + src.Uint64Range(0, 1_500_000_000_000_000-1)}
	}

	// nominally 1/3 also set account metadata via the transaction payload, applied
	// atomically with the tx. Target a posting's own destination: it already
	// passes the posting chart check when the tx commits, so the account-metadata
	// write never introduces a chart rejection.
	if generation.Choice(src, []uint8{0, 1, 2}) == 0 {
		payload.AccountMetadata = map[string]*commonpb.MetadataMap{
			postings[n-1].GetDestination(): {Values: randomMetaMap(src)},
		}
	}

	return applyCreate(ledger, payload)
}

// applyCreate wraps a CreateTransactionPayload as a ledger Apply request.
func applyCreate(ledger string, payload *servicepb.CreateTransactionPayload, skippableReasons ...commonpb.ErrorReason) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger:           ledger,
				SkippableReasons: skippableReasons,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: payload,
					},
				},
			},
		},
	}
}

// generateRejectedTransaction builds a transaction the server must reject,
// cycling the create rejection branches that valid traffic never triggers.
// Returns nil when the chosen branch is not currently applicable (no committed
// reference to duplicate yet).
func generateRejectedTransaction(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	switch generation.Choice(src, []uint8{0, 1, 2}) {
	case 0:
		return emptyTransaction(ledger)
	case 1:
		return duplicateReferenceTransaction(src, ledger, ls)
	default:
		return overflowTransaction(src, ledger)
	}
}

// emptyTransaction carries no postings and no script, so admission rejects it as
// having no content source (VALIDATION).
func emptyTransaction(ledger string) *servicepb.Request {
	return applyCreate(ledger, &servicepb.CreateTransactionPayload{})
}

// duplicateReferenceTransaction reuses a committed reference; the FSM rejects it
// with TRANSACTION_REFERENCE_CONFLICT before any floor/chart check. Nil when the
// ledger holds no committed reference yet.
func duplicateReferenceTransaction(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	ref, _, ok := pickTxRef(ls)
	if !ok {
		return nil
	}

	return applyCreate(ledger, &servicepb.CreateTransactionPayload{
		Postings:  []*commonpb.Posting{commonpb.NewPosting("world", poolAddress(src), assets[0], big.NewInt(1))},
		Reference: ref,
	})
}

// overflowTransaction sends two near-maximal (2^255) amounts from world to the
// same account, so the running volume exceeds 2^256 and the server rejects with
// VOLUME_OVERFLOW (the world Output overflows on the second posting).
func overflowTransaction(src *generation.Source, ledger string) *servicepb.Request {
	half := new(big.Int).Lsh(big.NewInt(1), 255)
	dst := poolAddress(src)

	return applyCreate(ledger, &servicepb.CreateTransactionPayload{
		Postings: []*commonpb.Posting{
			commonpb.NewPosting("world", dst, assets[0], half),
			commonpb.NewPosting("world", dst, assets[0], half),
		},
	})
}

// AddAccountType at a random pool prefix with a freshly-rolled
// persistence. Collisions and remove-races are expected; the new
// persistence wins when the add succeeds, and both sides stay in
// lockstep.
func generateAddAccountType(src *generation.Source, ledger string) *servicepb.Request {
	name := poolName(src)
	pattern := name + ":{id}"
	persistence := pickPersistence(src)

	return &servicepb.Request{
		Type: &servicepb.Request_AddAccountType{
			AddAccountType: &servicepb.AddAccountTypeLedgerRequest{
				Ledger: ledger,
				AccountType: &commonpb.AccountType{
					Name:        name,
					Pattern:     pattern,
					Persistence: persistence,
				},
			},
		},
	}
}

// NORMAL nominally 1/2, EPHEMERAL nominally 1/3, TRANSIENT nominally 1/6 — both non-NORMAL kinds
// exercise distinct end-of-bulk machinery.
func pickPersistence(src *generation.Source) commonpb.AccountTypePersistence {
	switch generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5}) {
	case 0, 1:
		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL
	case 2:
		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT
	default:
		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL
	}
}

// Single-posting transaction with a fresh idempotency key. Transient
// bulk generators build fund/drain pairs sharing address+asset+color+amount.
func txRequest(ledger, src, dest, asset, color string, amount *big.Int, force bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewColoredPosting(src, dest, asset, color, amount),
							},
							Force: force,
						},
					},
				},
			},
		},
	}
}

// Drains the exact balance of some EPHEMERAL cell to "world" with
// Force=true so the cell lands at input==output and gets purged.
// Seeks to a random pool cell and scans forward (wrapping once) for a
// drainable one — a positive balance (input > output) on an EPHEMERAL-matched
// address — so the pick costs O(scan window), not a volume-table walk.
// Returns nil if the window holds no eligible cell.
func generateDrainTransaction(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	const scanCap = 64

	probe := oracle.VolumeKey{Address: poolAddress(src), Asset: generation.Choice(src, assets)}

	var (
		srcKey  oracle.VolumeKey
		balance uint256.Int
		found   bool
	)

	scanned := 0
	consider := func(key oracle.VolumeKey, vp oracle.VolumePair) bool {
		scanned++
		if vp.Input.Cmp(&vp.Output) > 0 {
			if t := ls.MatchAddress(key.Address); t != nil && t.Persistence == commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL {
				srcKey = key
				balance.Sub(&vp.Input, &vp.Output)
				found = true
			}
		}

		return !found && scanned < scanCap
	}

	for key, vp := range ls.Volumes().From(probe) {
		if !consider(key, vp) {
			break
		}
	}
	if !found && scanned < scanCap {
		for key, vp := range ls.Volumes().All() {
			if !consider(key, vp) {
				break
			}
		}
	}

	if !found {
		return nil
	}

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewColoredPosting(srcKey.Address, "world", srcKey.Asset, srcKey.Color, balance.ToBig()),
							},
							Force: true,
						},
					},
				},
			},
		},
	}
}

// RemoveAccountType for a random pool name.
func generateRemoveAccountType(src *generation.Source, ledger string) *servicepb.Request {
	return removeRequest(ledger, poolName(src))
}

// --- Metadata -------------------------------------------------------------

// accountTarget builds a metadata Target pointing at an account address.
func accountTarget(addr string) *commonpb.Target {
	return &commonpb.Target{
		Target: &commonpb.Target_Account{
			Account: &commonpb.TargetAccount{Addr: addr},
		},
	}
}

// "k2"-style key from the small metadata-key pool.
func metaKey(src *generation.Source) string {
	return fmt.Sprintf("k%d", generation.Choice(src, metaKeyPool))
}

// Picks a ledger-level op (nominally 1/5), a transaction-level op (nominally 1/5, falling back to
// account when no transaction exists yet), or an account-level op — each Add
// (nominally 2/3) vs Delete (nominally 1/3). A delete falls back to its add when the model holds no
// metadata of that kind yet.
func generateMetadataOp(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	switch generation.Choice(src, []uint8{0, 1, 2, 3, 4}) {
	case 0:
		if generation.Choice(src, []uint8{0, 1, 2}) == 0 {
			if req := generateDeleteLedgerMetadata(src, ledger, ls); req != nil {
				return req
			}
		}

		return generateSaveLedgerMetadata(src, ledger)
	case 1:
		if req := generateTxMetadataOp(src, ledger, ls); req != nil {
			return req
		}
	}

	if generation.Choice(src, []uint8{0, 1, 2}) == 0 {
		if req := generateDeleteMetadata(src, ledger, ls); req != nil {
			return req
		}
	}

	return generateAddMetadata(src, ledger)
}

// randomMetaValue picks a value across every MetadataValue wire kind — string,
// int64, uint64, bool, null, datetime — from the small pools. The server stores
// values verbatim, so each kind must round-trip unchanged regardless of any
// declared field type.
func randomMetaValue(src *generation.Source) *commonpb.MetadataValue {
	switch generation.Choice(src, []uint8{0, 1, 2, 3, 4, 5}) {
	case 0:
		return commonpb.NewIntValue(generation.Choice(src, metaIntPool))
	case 1:
		return commonpb.NewUintValue(generation.Choice(src, metaUintPool))
	case 2:
		return commonpb.NewBoolValue(generation.Choice(src, []uint8{0, 1}) == 0)
	case 3:
		return commonpb.NewNullValue(generation.Choice(src, metaNullOriginalPool))
	case 4:
		return commonpb.NewDatetimeValue(generation.Choice(src, metaDatetimePool))
	default:
		return commonpb.NewStringValue(generation.Choice(src, metaValuePool))
	}
}

// randomMetaMap builds a 1-2 key metadata map from the small key/value pools.
// Small pools make concurrent sets of the same key to different values frequent
// — the ordering chaos the model exists to check.
func randomMetaMap(src *generation.Source) map[string]*commonpb.MetadataValue {
	n := 1 + int(generation.Choice(src, []uint8{0, 1}))
	md := make(map[string]*commonpb.MetadataValue, n)
	for range n {
		md[metaKey(src)] = randomMetaValue(src)
	}

	return md
}

// SaveMetadata on a blindly-picked pool address (so the server resolves typing)
// with 1-2 keys from the small pools.
func generateAddMetadata(src *generation.Source, ledger string) *servicepb.Request {
	addr := poolAddress(src)
	md := randomMetaMap(src)

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target:   accountTarget(addr),
							Metadata: md,
						},
					},
				},
			},
		},
	}
}

// DeleteMetadata of an existing (address, key) from the model — occasionally a
// key outside the write pool, so the miss that exercises METADATA_NOT_FOUND does
// not depend on a four-key pool failing to collide. Returns nil when the model
// holds no metadata.
func generateDeleteMetadata(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	chosen, _, ok := pickAtOrAfter(ls.Metadata(), oracle.MetaKey{Address: poolAddress(src), Key: metaKey(src)})
	if !ok {
		return nil
	}

	addr, key := chosen.Address, chosen.Key

	// Half the misses opt into the skip so the reason is reached at the rate the
	// branch is rolled; the rest stay bare, where the miss rejects the bulk.
	var skippable []commonpb.ErrorReason
	if generation.Choice(src, []uint8{0, 1}) == 0 {
		key = "absent-" + metaKey(src)
		if generation.Choice(src, []uint8{0, 1}) == 0 {
			skippable = []commonpb.ErrorReason{commonpb.ErrorReason_ERROR_REASON_METADATA_NOT_FOUND}
		}
	}

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger:           ledger,
				SkippableReasons: skippable,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_DeleteMetadata{
						DeleteMetadata: &commonpb.DeleteMetadataCommand{
							Target: accountTarget(addr),
							Key:    key,
						},
					},
				},
			},
		},
	}
}

// generateTxMetadataOp targets a committed transaction by reference: Delete
// (nominally 1/3) of an existing (reference, key), else Add. Returns nil when the model
// holds no transactions yet (caller falls back to account metadata).
func generateTxMetadataOp(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	if generation.Choice(src, []uint8{0, 1, 2}) == 0 {
		if req := generateDeleteTxMetadata(src, ledger, ls); req != nil {
			return req
		}
	}

	return generateAddTxMetadata(src, ledger, ls)
}

// generateAddTxMetadata sets 1-2 metadata keys on a committed transaction picked
// by reference. Returns nil when no transaction exists yet.
func generateAddTxMetadata(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	_, id, ok := pickTxRef(ls)
	if !ok {
		return nil
	}

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target:   txTarget(uint64(id)),
							Metadata: randomMetaMap(src),
						},
					},
				},
			},
		},
	}
}

// generateDeleteTxMetadata deletes a metadata key from a committed transaction —
// occasionally a freshly-rolled key on a known reference to exercise
// METADATA_NOT_FOUND. Seek-picks a few transactions and returns nil when none
// of them carries metadata.
func generateDeleteTxMetadata(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	txs := ls.Txs()
	for range 8 {
		_, id, ok := pickTxRef(ls)
		if !ok {
			return nil
		}

		md := txs.Get(id - 1).Metadata()
		if len(md) == 0 {
			continue
		}

		// Per-record map, a couple of entries: collect+sort keeps the pick
		// Antithesis-steerable at negligible cost.
		keys := make([]string, 0, len(md))
		for k := range md {
			keys = append(keys, k)
		}
		slices.Sort(keys)

		key := generation.Choice(src, keys)
		if generation.Choice(src, []uint8{0, 1, 2, 3}) == 0 {
			key = "absent-" + metaKey(src)
		}

		return &servicepb.Request{
			Type: &servicepb.Request_Apply{
				Apply: &servicepb.LedgerApplyRequest{
					Ledger: ledger,
					Action: &servicepb.LedgerAction{
						Data: &servicepb.LedgerAction_DeleteMetadata{
							DeleteMetadata: &commonpb.DeleteMetadataCommand{
								Target: txTarget(uint64(id)),
								Key:    key,
							},
						},
					},
				},
			},
		}
	}

	return nil
}

// generateRevert reverts a committed transaction by reference, nominally half with force
// and nominally half at the original's effective date (the revert then inherits the
// original's timestamp — verifiable when the original carried a user-supplied
// one). A non-forced revert applies the balance floor to the reversed postings —
// the reversed source is the original destination, which may since have spent
// (or been purged of) the funds — so it also exercises the revert path's
// INSUFFICIENT_FUNDS rejection. The server validates account types before
// the floor on reverts, so the oracle does the same (see applyRevert).
// Post-commit volumes are returned unconditionally on the transaction for
// validation. Targeting any committed reference exercises both the success path
// and the TRANSACTION_ALREADY_REVERTED rejection (a reference picked after a
// prior revert committed).
func generateRevert(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	_, id, ok := pickTxRef(ls)
	if !ok {
		return nil
	}

	payload := &servicepb.RevertTransactionPayload{
		TransactionId: uint64(id),
		Force:         generation.Choice(src, []uint8{0, 1}) == 0,
		// nominally half at the original's effective date: the revert inherits the
		// original's timestamp instead of the server's current date.
		AtEffectiveDate: generation.Choice(src, []uint8{0, 1}) == 0,
	}

	// nominally half the reverts carry metadata on the revert transaction, echoed
	// verbatim on its log (see validateBulkSuccess's revert check).
	if generation.Choice(src, []uint8{0, 1}) == 0 {
		payload.Metadata = randomMetaMap(src)
	}

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_RevertTransaction{
						RevertTransaction: payload,
					},
				},
			},
		},
	}
}

// pickTxRef returns a random committed transaction as (reference, id), or
// ok=false when the model holds none. References carry uniform random hex
// suffixes, so seeking to a freshly-rolled one lands on an existing entry
// near-uniformly in O(log n).
func pickTxRef(ls oracle.LedgerState) (ref string, id int, ok bool) {
	return pickAtOrAfter(ls.TxByRef(), txRef())
}

// txRef returns a globally-unique transaction reference, so no two transactions
// ever collide on it (sidestepping TRANSACTION_REFERENCE_CONFLICT modeling).
func txRef() string {
	return fmt.Sprintf("t-ref:%016x%016x", internal.Rand().Uint64(), internal.Rand().Uint64())
}

// txTarget builds a transaction metadata target addressed by id (the proto
// dropped reference targeting in #462; the caller resolves a tracked reference
// to its id).
func txTarget(id uint64) *commonpb.Target {
	return &commonpb.Target{
		Target: &commonpb.Target_TransactionId{TransactionId: id},
	}
}

// SaveLedgerMetadata with 1-2 keys from the small pools. The key space is shared
// across all workers on the ledger, so concurrent sets of the same key to
// different values are frequent — the ledger-level ordering chaos.
func generateSaveLedgerMetadata(src *generation.Source, ledger string) *servicepb.Request {
	md := randomMetaMap(src)

	return &servicepb.Request{
		Type: &servicepb.Request_SaveLedgerMetadata{
			SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{
				Ledger:   ledger,
				Metadata: md,
			},
		},
	}
}

// DeleteLedgerMetadata of an existing key from the model — occasionally a
// freshly-rolled key to exercise METADATA_NOT_FOUND. Returns nil when the ledger
// holds no metadata.
func generateDeleteLedgerMetadata(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	// LedgerMeta().All() iterates in key order, so keys is already sorted.
	keys := make([]string, 0, ls.LedgerMeta().Len())
	for k := range ls.LedgerMeta().All() {
		keys = append(keys, k)
	}

	if len(keys) == 0 {
		return nil
	}

	key := generation.Choice(src, keys)
	if generation.Choice(src, []uint8{0, 1, 2, 3}) == 0 {
		key = metaKey(src)
	}

	return &servicepb.Request{
		Type: &servicepb.Request_DeleteLedgerMetadata{
			DeleteLedgerMetadata: &servicepb.DeleteLedgerMetadataRequest{
				Ledger: ledger,
				Key:    key,
			},
		},
	}
}

// Picks Set (nominally 3/4) vs Remove (nominally 1/4) of a metadata field type. Remove falls back
// to Set when the model declares no field types yet.
func generateSchemaOp(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	if generation.Choice(src, []uint8{0, 1, 2, 3}) == 0 {
		if req := generateRemoveMetadataFieldType(src, ledger, ls); req != nil {
			return req
		}
	}

	return generateSetMetadataFieldType(src, ledger, ls)
}

// SetMetadataFieldType for an account- or ledger-target key, with a type from the
// pool. Declared on the same small key pool as metadata values, so writes and
// reads of those keys coerce.
//
// A retype of a key that currently has a metadata index triggers a background
// index REWRITE (version bump + re-encode). The model tracks its serving
// window (oracle.RetypeWindow): until the driver observes every replica's
// atomic switch, queries on the key are legal under the old type or the new,
// each as a whole window — so indexed keys are retyped like any other.
func generateSetMetadataFieldType(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	target := generation.Choice(src, metaTargetPool)
	key := metaKey(src)

	return &servicepb.Request{
		Type: &servicepb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: target,
				Key:        key,
				Type:       generation.Choice(src, metaTypePool),
			},
		},
	}
}

// RemoveMetadataFieldType for a declared (target, key) from the model —
// occasionally a freshly-rolled one (a no-op on the server). Returns nil when the
// model declares no field types.
func generateRemoveMetadataFieldType(src *generation.Source, ledger string, ls oracle.LedgerState) *servicepb.Request {
	type fieldRef struct {
		target commonpb.TargetType
		key    string
	}

	// Each table iterates in key order and the tables are visited in a fixed
	// sequence, so fields is deterministic.
	var fields []fieldRef
	for k := range ls.AccountFieldTypes().All() {
		fields = append(fields, fieldRef{commonpb.TargetType_TARGET_TYPE_ACCOUNT, k})
	}
	for k := range ls.LedgerFieldTypes().All() {
		fields = append(fields, fieldRef{commonpb.TargetType_TARGET_TYPE_LEDGER, k})
	}
	for k := range ls.TransactionFieldTypes().All() {
		fields = append(fields, fieldRef{commonpb.TargetType_TARGET_TYPE_TRANSACTION, k})
	}

	if len(fields) == 0 {
		return nil
	}

	chosen := generation.Choice(src, fields)

	target, key := chosen.target, chosen.key
	if generation.Choice(src, []uint8{0, 1, 2, 3}) == 0 {
		target = generation.Choice(src, metaTargetPool)
		key = metaKey(src)
	}

	return &servicepb.Request{
		Type: &servicepb.Request_RemoveMetadataFieldType{
			RemoveMetadataFieldType: &servicepb.RemoveMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: target,
				Key:        key,
			},
		},
	}
}

// 2-request fund+drain pair against a TRANSIENT-typed cell — exercises
// the end-of-batch transient zero check. Returns nil if no TRANSIENT
// type exists (other generators still cover other surfaces).
func generateTransientBalancedBulk(src *generation.Source, ledger string, ls oracle.LedgerState) []*servicepb.Request {
	t := randomTransientType(src, ls)
	if t == nil {
		return nil
	}

	dest, err := sampleAddress(src, t.Pattern)
	if err != nil {
		return nil
	}

	asset := assets[int(generation.Choice(src, []uint8{0, 1, 2}))]
	color := randomColor(src)
	amount := internal.GeneratedBigInt(src)

	return []*servicepb.Request{
		txRequest(ledger, "world", dest, asset, color, amount, true),
		txRequest(ledger, dest, "world", asset, color, amount, true),
	}
}

// Single fund of a TRANSIENT cell, no drain — server is expected to
// reject the bulk with ErrTransientAccountNonZero.
func generateTransientUnbalancedBulk(src *generation.Source, ledger string, ls oracle.LedgerState) []*servicepb.Request {
	t := randomTransientType(src, ls)
	if t == nil {
		return nil
	}

	dest, err := sampleAddress(src, t.Pattern)
	if err != nil {
		return nil
	}

	asset := assets[int(generation.Choice(src, []uint8{0, 1, 2}))]
	amount := internal.GeneratedBigInt(src)

	return []*servicepb.Request{
		txRequest(ledger, "world", dest, asset, randomColor(src), amount, true),
	}
}

func removeRequest(ledger, name string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_RemoveAccountType{
			RemoveAccountType: &servicepb.RemoveAccountTypeLedgerRequest{
				Ledger: ledger,
				Name:   name,
			},
		},
	}
}

// idempotencyKeyNamespace prefixes every key this driver mints, so a prefix
// query on it selects the whole run and nothing a neighbouring workload wrote.
const idempotencyKeyNamespace = "model-"

// Fresh unique key per Request. The server caches log refs by
// (key, order), making gRPC retries of non-idempotent actions safe.
func idempotencyKey() string {
	return fmt.Sprintf("%s%016x%016x", idempotencyKeyNamespace, internal.Rand().Uint64(), internal.Rand().Uint64())
}

// Concrete address matching pattern; variable segments get random
// numeric ids.
func sampleAddress(src *generation.Source, pattern string) (string, error) {
	segs, err := accounttype.ParsePattern(pattern)
	if err != nil {
		return "", err
	}

	parts := make([]string, len(segs))

	for i, seg := range segs {
		switch seg.Kind {
		case accounttype.SegmentFixed:
			parts[i] = seg.Value
		case accounttype.SegmentVariable:
			// Small range so addresses recur and volumes accumulate.
			parts[i] = strconv.FormatUint(src.Uint64Range(0, 1000-1), 10)
		}
	}

	return strings.Join(parts, ":"), nil
}
