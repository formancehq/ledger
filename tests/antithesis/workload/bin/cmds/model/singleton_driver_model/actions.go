package main

import (
	"fmt"
	"math/big"
	"slices"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/holiman/uint256"

	"github.com/formancehq/ledger/v3/internal/domain/accounttype"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// applyRequest renders a bulk into a sendable ApplyRequest. One fresh
// idempotency key for the whole batch (idempotency is per ApplyBatch),
// generated once per Apply call and reused across the client's internal
// retries, so an ambiguous UNAVAILABLE replays the committed batch instead of
// re-applying it.
func applyRequest(b oracle.Bulk) *servicepb.ApplyRequest {
	return servicepb.UnsignedApplyRequest(idempotencyKey(), b.Requests...)
}

// Pool: every type name and every tx address uses a "t-N" prefix where
// N comes from typePool. The pool is small on purpose — collisions
// (race-add, race-remove, untyped tx) are the test's chaos surface.

// "t-3"-style name.
func poolName() string {
	return fmt.Sprintf("t-%d", random.RandomChoice(typePool))
}

// "t-N:i" — same prefix space as type names. numIDsPerPrefix is
// small enough that re-targeting the same cell is frequent.
func poolAddress() string {
	return fmt.Sprintf("%s:%d", poolName(), internal.Rand().Uint64()%numIDsPerPrefix)
}

// sourceAddress picks a debit source without filtering on balance: "world"
// (overdraftable) about half the time to keep accounts funded, otherwise any
// pool address — most of which are underfunded, so a non-forced debit exercises
// the INSUFFICIENT_FUNDS floor.
func sourceAddress() string {
	if random.RandomChoice([]uint8{0, 1}) == 0 {
		return "world"
	}

	return poolAddress()
}

// generateBulk plans the next bulk. Most bulks target a single ledger;
// occasionally a bulk spreads its requests across a few, exercising the
// server's atomic-across-ledgers semantics. Reads the committed state but
// never mutates it.
func generateBulk(g oracle.GlobalState, ledgers []string, receipts map[string]string) oracle.Bulk {
	picks := pickLedgers(ledgers)

	// Whole-bulk transient shapes fund and drain the same cell, so they only
	// make sense single-ledger.
	if len(picks) == 1 {
		ledger := picks[0]
		ls := g.Ledger(ledger)
		switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11}) {
		case 0:
			if reqs := generateTransientBalancedBulk(ledger, ls); reqs != nil {
				return oracle.Bulk{Requests: reqs}
			}
		case 1:
			if reqs := generateTransientUnbalancedBulk(ledger, ls); reqs != nil {
				return oracle.Bulk{Requests: reqs}
			}
		}
	}

	size := bulkSize()
	requests := make([]*servicepb.Request, 0, size)

	for i := 0; i < size; i++ {
		ledger := random.RandomChoice(picks)
		ls := g.Ledger(ledger)

		if rollChartOp() {
			if req := generateChartOp(ledger); req != nil {
				requests = append(requests, req)
				continue
			}
		}

		if rollSchemaOp() {
			if req := generateSchemaOp(ledger, ls); req != nil {
				requests = append(requests, req)
				continue
			}
		}

		if rollMetadataOp() {
			if req := generateMetadataOp(ledger, ls); req != nil {
				requests = append(requests, req)
				continue
			}
		}

		if rollRevert() {
			if req := generateRevert(ledger, ls, receipts); req != nil {
				requests = append(requests, req)
				continue
			}
		}

		if rollTransaction(ls) {
			if req := generateTransaction(ledger, ls); req != nil {
				requests = append(requests, req)
				continue
			}
		}

		// Transaction back-pressure kicked in: emit a metadata op instead so the
		// slot stays productive and the workload exercises existing state rather
		// than creating ever more transactions as the ledger fills up.
		if req := generateMetadataOp(ledger, ls); req != nil {
			requests = append(requests, req)
		}
	}

	return oracle.Bulk{Requests: requests}
}

// rollTransaction reports whether to create a new transaction, tapering with the
// ledger's committed-transaction count (see the txEmit* knobs) so tracked
// references don't grow without bound.
func rollTransaction(ls oracle.LedgerState) bool {
	switch n := len(ls.TxByRef()); {
	case n < txEmitFull:
		return true
	case n < txEmitTaper:
		return random.RandomChoice([]uint8{0, 1}) == 0
	case n < txEmitStop:
		return random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7}) == 0
	default:
		return false
	}
}

// pickLedgers returns the ledger(s) a bulk's requests draw from: usually one,
// ~1-in-6 a distinct 2-3 so the bulk spans ledgers.
func pickLedgers(ledgers []string) []string {
	if len(ledgers) < 2 || random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) != 0 {
		return []string{random.RandomChoice(ledgers)}
	}

	n := 2
	if len(ledgers) >= 3 && random.RandomChoice([]uint8{0, 1}) == 0 {
		n = 3
	}

	return sampleDistinct(ledgers, n)
}

// sampleDistinct picks n distinct ledgers via the Antithesis RNG (rejection
// sampling; n <= len(ledgers) so it terminates).
func sampleDistinct(ledgers []string, n int) []string {
	seen := map[string]bool{}
	out := make([]string, 0, n)
	for len(out) < n {
		c := random.RandomChoice(ledgers)
		if !seen[c] {
			seen[c] = true
			out = append(out, c)
		}
	}

	return out
}

// ~30%: a chart op fills this bulk slot, else a transaction.
func rollChartOp() bool {
	return random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}) < 3
}

// ~1-in-6: a metadata-schema op (declare/remove a field type) fills this slot.
func rollSchemaOp() bool {
	return random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) == 0
}

// ~25%: a metadata op fills this bulk slot, when a chart/schema op didn't.
func rollMetadataOp() bool {
	return random.RandomChoice([]uint8{0, 1, 2, 3}) == 0
}

// ~1-in-6: revert a committed transaction in this slot.
func rollRevert() bool {
	return random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) == 0
}

// Picks Add vs Remove.
func generateChartOp(ledger string) *servicepb.Request {
	if random.RandomChoice([]uint8{0, 1}) == 0 {
		return generateAddAccountType(ledger)
	}

	return generateRemoveAccountType(ledger)
}

// Heavy tail toward 1: 5/8 single, 2/8 pair, 1/8 in {3,4,5}.
func bulkSize() int {
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7}) {
	case 0, 1, 2, 3, 4:
		return 1
	case 5, 6:
		return 2
	default:
		return int(random.RandomChoice([]uint8{3, 4, 5}))
	}
}

// randomTransientType returns a random TRANSIENT type from ls's chart, or nil.
func randomTransientType(ls oracle.LedgerState) *oracle.TypeState {
	names := make([]string, 0, len(ls.Types()))
	for name, t := range ls.Types() {
		if t.Persistence == commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT {
			names = append(names, name)
		}
	}

	if len(names) == 0 {
		return nil
	}

	slices.Sort(names)

	t := ls.Types()[random.RandomChoice(names)]

	return &t
}

// --- Per-action generators ------------------------------------------------

// A chaos transaction of 1–4 postings between blindly-picked pool addresses
// (source may be "world"), amounts and force rolled at random so the server
// resolves typing, the balance floor, intra-transaction posting composition,
// and — with force — overdrafts uniformly; or, ~1/4 of the time, a deliberate
// drain of an EPHEMERAL cell to exercise the purge sweep.
func generateTransaction(ledger string, ls oracle.LedgerState) *servicepb.Request {
	if random.RandomChoice([]uint8{0, 1, 2, 3}) == 0 {
		if req := generateDrainTransaction(ledger, ls); req != nil {
			return req
		}
	}

	// Force (~1/8) skips the balance floor, letting a non-world source overdraft.
	force := random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5, 6, 7}) == 0

	// 1–4 postings, each between blindly-picked accounts. Multiple postings
	// commit atomically and compose in order — an earlier posting can fund a
	// later one's source (the running balance floor is per-posting).
	n := 1 + int(random.RandomChoice([]uint8{0, 1, 2, 3}))
	postings := make([]*commonpb.Posting, n)
	for i := range postings {
		postings[i] = commonpb.NewPosting(sourceAddress(), poolAddress(), assets[int(random.RandomChoice([]uint8{0, 1, 2}))], internal.RandomBigInt())
	}

	// Every transaction gets a unique reference so it is targetable by later
	// transaction-metadata writes. ~half also carry metadata at creation.
	payload := &servicepb.CreateTransactionPayload{
		Postings:  postings,
		Reference: txRef(),
		Force:     force,
		// Need PCV for validation.
		ExpandVolumes: true,
	}

	if random.RandomChoice([]uint8{0, 1}) == 0 {
		payload.Metadata = randomMetaMap()
	}

	// ~half also carry a user-supplied timestamp — stored verbatim and echoed on
	// reads (it does not feed the HLC / log order). Backdated to stay non-future.
	if random.RandomChoice([]uint8{0, 1}) == 0 {
		payload.Timestamp = &commonpb.Timestamp{Data: 1 + internal.Rand().Uint64()%1_500_000_000_000_000}
	}

	// ~1/3 also set account metadata via the transaction payload, applied
	// atomically with the tx. Target a posting's own destination: it already
	// passes the posting chart check when the tx commits, so the account-metadata
	// write never introduces a chart rejection.
	if random.RandomChoice([]uint8{0, 1, 2}) == 0 {
		payload.AccountMetadata = map[string]*commonpb.MetadataMap{
			postings[n-1].GetDestination(): {Values: randomMetaMap()},
		}
	}

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: payload,
					},
				},
			},
		},
	}
}

// AddAccountType at a random pool prefix with a freshly-rolled
// persistence. Collisions and remove-races are expected; the new
// persistence wins when the add succeeds, and both sides stay in
// lockstep.
func generateAddAccountType(ledger string) *servicepb.Request {
	name := poolName()
	pattern := name + ":{id}"
	persistence := pickPersistence()

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

// NORMAL ~1/2, EPHEMERAL ~1/3, TRANSIENT ~1/6 — both non-NORMAL kinds
// exercise distinct end-of-bulk machinery.
func pickPersistence() commonpb.AccountTypePersistence {
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) {
	case 0, 1:
		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL
	case 2:
		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_TRANSIENT
	default:
		return commonpb.AccountTypePersistence_ACCOUNT_TYPE_NORMAL
	}
}

// Single-posting transaction with a fresh idempotency key. Transient
// bulk generators build fund/drain pairs sharing address+asset+amount.
func txRequest(ledger, src, dest, asset string, amount *big.Int, force bool) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewPosting(src, dest, asset, amount),
							},
							Force:         force,
							ExpandVolumes: true,
						},
					},
				},
			},
		},
	}
}

// Drains the exact balance of some EPHEMERAL cell to "world" with
// Force=true so the cell lands at input==output and gets purged.
// Returns nil if no eligible cell exists.
func generateDrainTransaction(ledger string, ls oracle.LedgerState) *servicepb.Request {
	// Collect every drainable cell, then pick via the Antithesis RNG over a
	// sorted slice — replayable / steerable, unlike map-iteration order.
	type drainCandidate struct {
		key     oracle.VolumeKey
		balance uint256.Int
	}

	var candidates []drainCandidate
	for key, vp := range ls.Volumes() {
		t := ls.MatchAddress(key.Address)
		if t == nil {
			continue
		}
		if t.Persistence != commonpb.AccountTypePersistence_ACCOUNT_TYPE_EPHEMERAL {
			continue
		}

		// Drainable only with a positive balance (input > output).
		if vp.Input.Cmp(&vp.Output) <= 0 {
			continue
		}

		var bal uint256.Int
		bal.Sub(&vp.Input, &vp.Output)

		candidates = append(candidates, drainCandidate{key: key, balance: bal})
	}

	if len(candidates) == 0 {
		return nil
	}

	slices.SortFunc(candidates, func(a, b drainCandidate) int {
		return oracle.CompareVolumeKey(a.key, b.key)
	})

	chosen := random.RandomChoice(candidates)
	srcKey, balance := chosen.key, chosen.balance

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_CreateTransaction{
						CreateTransaction: &servicepb.CreateTransactionPayload{
							Postings: []*commonpb.Posting{
								commonpb.NewPosting(srcKey.Address, "world", srcKey.Asset, balance.ToBig()),
							},
							Force:         true,
							ExpandVolumes: true,
						},
					},
				},
			},
		},
	}
}

// RemoveAccountType for a random pool name.
func generateRemoveAccountType(ledger string) *servicepb.Request {
	return removeRequest(ledger, poolName())
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
func metaKey() string {
	return fmt.Sprintf("k%d", random.RandomChoice(metaKeyPool))
}

// Picks a ledger-level op (~1/5), a transaction-level op (~1/5, falling back to
// account when no transaction exists yet), or an account-level op — each Add
// (~2/3) vs Delete (~1/3). A delete falls back to its add when the model holds no
// metadata of that kind yet.
func generateMetadataOp(ledger string, ls oracle.LedgerState) *servicepb.Request {
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4}) {
	case 0:
		if random.RandomChoice([]uint8{0, 1, 2}) == 0 {
			if req := generateDeleteLedgerMetadata(ledger, ls); req != nil {
				return req
			}
		}

		return generateSaveLedgerMetadata(ledger)
	case 1:
		if req := generateTxMetadataOp(ledger, ls); req != nil {
			return req
		}
	}

	if random.RandomChoice([]uint8{0, 1, 2}) == 0 {
		if req := generateDeleteMetadata(ledger, ls); req != nil {
			return req
		}
	}

	return generateAddMetadata(ledger)
}

// randomMetaValue picks a value across every MetadataValue wire kind — string,
// int64, uint64, bool, null, datetime — from the small pools. The server stores
// values verbatim, so each kind must round-trip unchanged regardless of any
// declared field type.
func randomMetaValue() *commonpb.MetadataValue {
	switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) {
	case 0:
		return commonpb.NewIntValue(random.RandomChoice(metaIntPool))
	case 1:
		return commonpb.NewUintValue(random.RandomChoice(metaUintPool))
	case 2:
		return commonpb.NewBoolValue(random.RandomChoice([]uint8{0, 1}) == 0)
	case 3:
		return commonpb.NewNullValue(random.RandomChoice(metaNullOriginalPool))
	case 4:
		return commonpb.NewDatetimeValue(random.RandomChoice(metaDatetimePool))
	default:
		return commonpb.NewStringValue(random.RandomChoice(metaValuePool))
	}
}

// randomMetaMap builds a 1-2 key metadata map from the small key/value pools.
// Small pools make concurrent sets of the same key to different values frequent
// — the ordering chaos the model exists to check.
func randomMetaMap() map[string]*commonpb.MetadataValue {
	n := 1 + int(random.RandomChoice([]uint8{0, 1}))
	md := make(map[string]*commonpb.MetadataValue, n)
	for i := 0; i < n; i++ {
		md[metaKey()] = randomMetaValue()
	}

	return md
}

// SaveMetadata on a blindly-picked pool address (so the server resolves typing)
// with 1-2 keys from the small pools.
func generateAddMetadata(ledger string) *servicepb.Request {
	addr := poolAddress()
	md := randomMetaMap()

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
// freshly-rolled key on that address to exercise METADATA_NOT_FOUND. Returns nil
// when the model holds no metadata.
func generateDeleteMetadata(ledger string, ls oracle.LedgerState) *servicepb.Request {
	keys := make([]oracle.MetaKey, 0, len(ls.Metadata()))
	for mk := range ls.Metadata() {
		keys = append(keys, mk)
	}

	if len(keys) == 0 {
		return nil
	}

	slices.SortFunc(keys, oracle.CompareMetaKey)
	chosen := random.RandomChoice(keys)

	addr, key := chosen.Address, chosen.Key
	if random.RandomChoice([]uint8{0, 1, 2, 3}) == 0 {
		key = metaKey()
	}

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
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
// (~1/3) of an existing (reference, key), else Add. Returns nil when the model
// holds no transactions yet (caller falls back to account metadata).
func generateTxMetadataOp(ledger string, ls oracle.LedgerState) *servicepb.Request {
	if random.RandomChoice([]uint8{0, 1, 2}) == 0 {
		if req := generateDeleteTxMetadata(ledger, ls); req != nil {
			return req
		}
	}

	return generateAddTxMetadata(ledger, ls)
}

// generateAddTxMetadata sets 1-2 metadata keys on a committed transaction picked
// by reference. Returns nil when no transaction exists yet.
func generateAddTxMetadata(ledger string, ls oracle.LedgerState) *servicepb.Request {
	ref := pickTxRef(ls)
	if ref == "" {
		return nil
	}

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_AddMetadata{
						AddMetadata: &commonpb.SaveMetadataCommand{
							Target:   txTarget(uint64(ls.TxByRef()[ref])),
							Metadata: randomMetaMap(),
						},
					},
				},
			},
		},
	}
}

// generateDeleteTxMetadata deletes a metadata key from a committed transaction —
// occasionally a freshly-rolled key on a known reference to exercise
// METADATA_NOT_FOUND. Returns nil when the model holds no transaction metadata.
func generateDeleteTxMetadata(ledger string, ls oracle.LedgerState) *servicepb.Request {
	type refKey struct {
		ref string
		key string
	}

	var keys []refKey
	txs := ls.Txs()
	for ref, id := range ls.TxByRef() {
		for k := range txs[id-1].Metadata() {
			keys = append(keys, refKey{ref: ref, key: k})
		}
	}

	if len(keys) == 0 {
		return nil
	}

	slices.SortFunc(keys, func(a, b refKey) int {
		if a.ref != b.ref {
			return strings.Compare(a.ref, b.ref)
		}
		return strings.Compare(a.key, b.key)
	})
	chosen := random.RandomChoice(keys)

	ref, key := chosen.ref, chosen.key
	if random.RandomChoice([]uint8{0, 1, 2, 3}) == 0 {
		key = metaKey()
	}

	return &servicepb.Request{
		Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{
				Ledger: ledger,
				Action: &servicepb.LedgerAction{
					Data: &servicepb.LedgerAction_DeleteMetadata{
						DeleteMetadata: &commonpb.DeleteMetadataCommand{
							Target: txTarget(uint64(ls.TxByRef()[ref])),
							Key:    key,
						},
					},
				},
			},
		},
	}
}

// generateRevert reverts a committed transaction by reference, ~half with force
// and ~half at the original's effective date (the revert then inherits the
// original's timestamp — verifiable when the original carried a user-supplied
// one). A non-forced revert applies the balance floor to the reversed postings —
// the reversed source is the original destination, which may since have spent
// (or been purged of) the funds — so it also exercises the revert path's
// INSUFFICIENT_FUNDS rejection. The server validates account types before
// the floor on reverts, so the oracle does the same (see applyRevert).
// expand_volumes returns post-commit volumes for validation. Targeting any
// committed reference exercises both the success path and the
// TRANSACTION_ALREADY_REVERTED rejection (a reference picked after a prior
// revert committed).
func generateRevert(ledger string, ls oracle.LedgerState, receipts map[string]string) *servicepb.Request {
	ref := pickTxRef(ls)
	if ref == "" {
		return nil
	}

	payload := &servicepb.RevertTransactionPayload{
		TransactionId: uint64(ls.TxByRef()[ref]),
		Force:         random.RandomChoice([]uint8{0, 1}) == 0,
		// ~half at the original's effective date: the revert inherits the
		// original's timestamp instead of the server's current date.
		AtEffectiveDate: random.RandomChoice([]uint8{0, 1}) == 0,
		ExpandVolumes:   true,
	}

	// ~half the reverts with a captured receipt carry it, exercising admission's
	// receipt path: it verifies the JWT and reverses the receipt's claimed
	// postings, bypassing the transaction-state store fetch. Outcome matches the
	// store path for a valid receipt, so the model needs no change.
	if receipt := receipts[ref]; receipt != "" && random.RandomChoice([]uint8{0, 1}) == 0 {
		payload.Receipt = receipt
	}

	// ~half the reverts carry metadata on the revert transaction, echoed
	// verbatim on its log (see validateBulkSuccess's revert check).
	if random.RandomChoice([]uint8{0, 1}) == 0 {
		payload.Metadata = randomMetaMap()
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

// pickTxRef returns a deterministically-chosen committed transaction reference,
// or "" when the model holds none.
func pickTxRef(ls oracle.LedgerState) string {
	refs := make([]string, 0, len(ls.TxByRef()))
	for r := range ls.TxByRef() {
		refs = append(refs, r)
	}

	if len(refs) == 0 {
		return ""
	}

	slices.Sort(refs)

	return random.RandomChoice(refs)
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
func generateSaveLedgerMetadata(ledger string) *servicepb.Request {
	md := randomMetaMap()

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
func generateDeleteLedgerMetadata(ledger string, ls oracle.LedgerState) *servicepb.Request {
	keys := make([]string, 0, len(ls.LedgerMeta()))
	for k := range ls.LedgerMeta() {
		keys = append(keys, k)
	}

	if len(keys) == 0 {
		return nil
	}

	slices.Sort(keys)
	key := random.RandomChoice(keys)
	if random.RandomChoice([]uint8{0, 1, 2, 3}) == 0 {
		key = metaKey()
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

// Picks Set (~3/4) vs Remove (~1/4) of a metadata field type. Remove falls back
// to Set when the model declares no field types yet.
func generateSchemaOp(ledger string, ls oracle.LedgerState) *servicepb.Request {
	if random.RandomChoice([]uint8{0, 1, 2, 3}) == 0 {
		if req := generateRemoveMetadataFieldType(ledger, ls); req != nil {
			return req
		}
	}

	return generateSetMetadataFieldType(ledger)
}

// SetMetadataFieldType for an account- or ledger-target key, with a type from the
// pool. Declared on the same small key pool as metadata values, so writes and
// reads of those keys coerce.
func generateSetMetadataFieldType(ledger string) *servicepb.Request {
	return &servicepb.Request{
		Type: &servicepb.Request_SetMetadataFieldType{
			SetMetadataFieldType: &servicepb.SetMetadataFieldTypeRequest{
				Ledger:     ledger,
				TargetType: random.RandomChoice(metaTargetPool),
				Key:        metaKey(),
				Type:       random.RandomChoice(metaTypePool),
			},
		},
	}
}

// RemoveMetadataFieldType for a declared (target, key) from the model —
// occasionally a freshly-rolled one (a no-op on the server). Returns nil when the
// model declares no field types.
func generateRemoveMetadataFieldType(ledger string, ls oracle.LedgerState) *servicepb.Request {
	type fieldRef struct {
		target commonpb.TargetType
		key    string
	}

	var fields []fieldRef
	for k := range ls.AccountFieldTypes() {
		fields = append(fields, fieldRef{commonpb.TargetType_TARGET_TYPE_ACCOUNT, k})
	}
	for k := range ls.LedgerFieldTypes() {
		fields = append(fields, fieldRef{commonpb.TargetType_TARGET_TYPE_LEDGER, k})
	}
	for k := range ls.TransactionFieldTypes() {
		fields = append(fields, fieldRef{commonpb.TargetType_TARGET_TYPE_TRANSACTION, k})
	}

	if len(fields) == 0 {
		return nil
	}

	slices.SortFunc(fields, func(a, b fieldRef) int {
		if a.target != b.target {
			return int(a.target) - int(b.target)
		}
		return strings.Compare(a.key, b.key)
	})
	chosen := random.RandomChoice(fields)

	target, key := chosen.target, chosen.key
	if random.RandomChoice([]uint8{0, 1, 2, 3}) == 0 {
		target = random.RandomChoice(metaTargetPool)
		key = metaKey()
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
func generateTransientBalancedBulk(ledger string, ls oracle.LedgerState) []*servicepb.Request {
	t := randomTransientType(ls)
	if t == nil {
		return nil
	}

	dest, err := sampleAddress(t.Pattern)
	if err != nil {
		return nil
	}

	asset := assets[int(random.RandomChoice([]uint8{0, 1, 2}))]
	amount := internal.RandomBigInt()

	return []*servicepb.Request{
		txRequest(ledger, "world", dest, asset, amount, true),
		txRequest(ledger, dest, "world", asset, amount, true),
	}
}

// Single fund of a TRANSIENT cell, no drain — server is expected to
// reject the bulk with ErrTransientAccountNonZero.
func generateTransientUnbalancedBulk(ledger string, ls oracle.LedgerState) []*servicepb.Request {
	t := randomTransientType(ls)
	if t == nil {
		return nil
	}

	dest, err := sampleAddress(t.Pattern)
	if err != nil {
		return nil
	}

	asset := assets[int(random.RandomChoice([]uint8{0, 1, 2}))]
	amount := internal.RandomBigInt()

	return []*servicepb.Request{
		txRequest(ledger, "world", dest, asset, amount, true),
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

// Fresh unique key per Request. The server caches log refs by
// (key, order), making gRPC retries of non-idempotent actions safe.
func idempotencyKey() string {
	return fmt.Sprintf("model-%016x%016x", internal.Rand().Uint64(), internal.Rand().Uint64())
}

// Concrete address matching pattern; variable segments get random
// numeric ids.
func sampleAddress(pattern string) (string, error) {
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
			parts[i] = fmt.Sprintf("%d", internal.Rand().Uint64()%1000)
		}
	}

	return strings.Join(parts, ":"), nil
}
