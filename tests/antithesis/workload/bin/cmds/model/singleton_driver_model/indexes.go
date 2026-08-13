package main

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/domain/indexes"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Index lifecycle. The query surface has index-backed conditions the compiler
// serves only from created indexes: AccountHasAsset (account-by-asset builtin)
// on accounts, and reference / date builtins on transactions. To exercise those
// paths with validated results (not just the not-found rejection), the
// generator emits CreateIndex / DropIndex bulks for each (workloadIndexes) and
// the model tracks each index's lifecycle:
//
//   - absent — no index. A has-asset query must be rejected (FailedPrecondition).
//   - ambiguous — created, but not yet confirmed READY on every replica. A query
//     may return a validated window OR be rejected not-ready, both legal (a
//     replica whose initial backfill hasn't flipped CurrentVersion>0 rejects it).
//   - active — the readiness poller confirmed CurrentVersion>0 on every reachable
//     replica. A query must return a validated window; a not-ready rejection is
//     now a finding.
//
// CreateIndex adds the index ambiguous; the poller promotes it to active once
// every replica reports it live, and demotes it back to ambiguous whenever a
// replica reports it not-ready again (a restored node rebuilding its read-store).
// DropIndex removes it instantly — the drop order sits in the committed prefix a
// MinLogSequence-pinned read must observe.

// assetIndexID is the account-by-asset builtin index the has-asset condition is
// served from. assetIndexCanonical is its stable map key.
func assetIndexID() *commonpb.IndexID {
	return indexes.AccountBuiltinID(commonpb.AccountBuiltinIndex_ACCT_BUILTIN_INDEX_ASSET)
}

var assetIndexCanonical = indexes.Canonical(assetIndexID())

// workloadTxBuiltins are the transaction builtin indexes the generator churns:
// the index-backed leaves of the transactions filter grammar (reference, the
// three date fields, and the three address-role account→tx mappings).
var workloadTxBuiltins = []commonpb.TransactionBuiltinIndex{
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS,
	commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS,
}

// addressRoleBuiltin maps an address role to the tx builtin index serving it —
// the model's copy of the compiler's txAddressIndexID.
func addressRoleBuiltin(role commonpb.AddressRole) commonpb.TransactionBuiltinIndex {
	switch role {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_SOURCE_ADDRESS
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_DESTINATION_ADDRESS
	default:
		return commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ADDRESS
	}
}

// txBuiltinCanonical returns the canonical IndexID string of a tx builtin — the
// model's index-map key the filter classifier and the lifecycle validation
// share.
func txBuiltinCanonical(field commonpb.TransactionBuiltinIndex) string {
	return indexes.Canonical(indexes.TxBuiltinID(field))
}

// workloadIndex is one index the generator churns: its wire ID and canonical key.
type workloadIndex struct {
	id        *commonpb.IndexID
	canonical string
}

// workloadIndexes is every index the workload creates and drops: the
// account-by-asset builtin plus the tx builtins.
func workloadIndexes() []workloadIndex {
	out := []workloadIndex{{assetIndexID(), assetIndexCanonical}}
	for _, field := range workloadTxBuiltins {
		out = append(out, workloadIndex{indexes.TxBuiltinID(field), txBuiltinCanonical(field)})
	}

	return out
}

// indexStateLabel renders an index's model lifecycle state for finding details.
func indexStateLabel(exists, active bool) string {
	switch {
	case !exists:
		return "absent"
	case active:
		return "active"
	default:
		return "ambiguous"
	}
}

// --- has-asset evaluation ------------------------------------------------

// workloadAsset is a (base, precision) pair matching one of the workload's asset
// strings (config.go assets), so a has-asset filter built from it can match the
// accounts those postings touched.
type workloadAsset struct {
	base      string
	precision uint32
}

// workloadAssets mirrors config.go assets {"USD/2","EUR/2","COIN"} split into
// (base, precision) via the server's asset-string convention (no "/N" suffix
// means precision 0).
var workloadAssets = []workloadAsset{{"USD", 2}, {"EUR", 2}, {"COIN", 0}}

// accountHasAsset reports whether addr is in the account-by-asset index for
// (assetBase, precision): whether it has EVER touched that asset via a committed,
// non-excluded posting. This is a monotonic history the oracle tracks (see
// LedgerState.everAsset / recordAssetTouches), NOT the current volume set — an
// account drained to zero and purged from the volume table still matches.
func accountHasAsset(ls oracle.LedgerState, addr, assetBase string, precision uint32) bool {
	return ls.HasEverAsset(addr, assetBase, precision)
}

// assetWindow is the model's prediction of a bare has-asset ListAccounts page:
// the accounts that ever touched (base, precision), in address order (reversed
// when reverse), with the exclusive cursor applied and capped at pageSize —
// mirroring accountWindow's pagination over the ever-touched set instead of the
// current-volume universe.
func assetWindow(ls oracle.LedgerState, base string, precision uint32, cursor string, pageSize int, reverse bool) []string {
	window := ls.EverAssetAccounts(base, precision)

	if reverse {
		reverseStrings(window)
	}

	if cursor != "" {
		kept := window[:0]
		for _, addr := range window {
			if reverse && addr >= cursor || !reverse && addr <= cursor {
				continue
			}

			kept = append(kept, addr)
		}
		window = kept
	}

	if len(window) > pageSize {
		window = window[:pageSize]
	}

	return window
}

// hasAssetTarget extracts the (base, precision) of a bare AccountHasAsset filter.
// genAccountAssetFilter only produces bare has-asset leaves, so this is total for
// the asset-index path.
func hasAssetTarget(f *commonpb.QueryFilter) (base string, precision uint32, ok bool) {
	if ha, isHA := f.GetFilter().(*commonpb.QueryFilter_AccountHasAsset); isHA {
		return ha.AccountHasAsset.GetAssetBase(), ha.AccountHasAsset.GetPrecision(), true
	}

	return "", 0, false
}

// --- asset-filter generation ---------------------------------------------

// genAccountAssetFilter rolls a bare AccountHasAsset leaf on a random workload
// asset — the account-by-asset lifecycle path. It is deliberately NOT composed
// with other leaves: the has-asset index returns accounts that may have been
// purged from the volume table ("ever touched"), while an index-free address leaf
// scans only current accounts, so a boolean of the two matches the server's
// iterator intersection/union — set semantics the model would have to reproduce
// the read-store to predict. A bare leaf's result set is exactly
// EverAssetAccounts(base, precision), which the model tracks directly.
func genAccountAssetFilter() *commonpb.QueryFilter {
	a := workloadAssets[int(random.RandomChoice([]uint8{0, 1, 2}))]

	return filterHasAsset(a.base, a.precision)
}

// --- asset-index query validation ----------------------------------------

// validateAssetAccountQuery validates a ListAccounts query whose only index need
// is the account-by-asset builtin. The legal outcomes depend on that index's
// lifecycle in each candidate base:
//
//   - a result set is legal iff some base holds the index (ambiguous or active)
//     and its ordered window equals the streamed accounts position-for-position;
//   - a not-ready rejection (FailedPrecondition — ErrIndexNotFound and
//     ErrIndexBuilding both map there) is legal iff some base does not hold the
//     index active (absent or ambiguous).
//
// Any other error code is a finding. So is a result set no base can produce
// (spurious rows without the index) and a rejection when every base has the index
// active (ready everywhere, yet rejected).
func (c *Checker) validateAssetAccountQuery(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, cursor string, pageSize int, reverse bool, serverAccts []*commonpb.Account, err error) {
	if err != nil && status.Code(err) != codes.FailedPrecondition {
		assert.Unreachable("singleton_driver_model: asset-index account query returned unexpected error", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	base, precision, ok := hasAssetTarget(filter)
	if !ok {
		assert.Unreachable("singleton_driver_model: asset-index query filter is not a bare has-asset leaf", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
		})

		return
	}

	gotResults := err == nil

	if c.matchesModel(maxTicket, "AQUERY-IDX", func(cand oracle.GlobalState) bool {
		ls := cand.Ledger(ledger)
		exists, active := ls.IndexState(assetIndexCanonical)

		if !gotResults {
			// A not-ready rejection is legal unless the index is active here.
			return !exists || !active
		}

		if !exists {
			return false // rows require the index present
		}

		want := assetWindow(ls, base, precision, cursor, pageSize, reverse)
		if len(want) != len(serverAccts) {
			return false
		}

		for i, addr := range want {
			if serverAccts[i].GetAddress() != addr || !accountMatches(ls, addr, serverAccts[i]) {
				return false
			}
		}

		return true
	}) {
		if gotResults {
			assert.Reachable("singleton_driver_model: asset-index account query served results", internal.Details{"ledger": ledger})
		} else {
			assert.Reachable("singleton_driver_model: asset-index account query gated", internal.Details{"ledger": ledger})
		}

		return
	}

	c.mu.Lock()
	modelLS := c.modelState.Ledger(ledger)
	exists, active := modelLS.IndexState(assetIndexCanonical)
	modelWindow := assetWindow(modelLS, base, precision, cursor, pageSize, reverse)
	c.mu.Unlock()

	details := internal.Details{
		"ledger":     ledger,
		"filter":     describeFilter(filter),
		"cursor":     cursor,
		"pageSize":   pageSize,
		"reverse":    reverse,
		"modelIdx":   indexStateLabel(exists, active),
		"modelAddrs": strings.Join(modelWindow, ","),
		"bases": c.describeCandidateVerdicts(maxTicket, ledger, map[string]struct{}{assetIndexCanonical: {}}, func(ls oracle.LedgerState) []string {
			return assetWindow(ls, base, precision, cursor, pageSize, reverse)
		}),
	}
	if gotResults {
		serverAddrs := make([]string, len(serverAccts))
		for i, a := range serverAccts {
			serverAddrs[i] = a.GetAddress()
		}
		details["rows"] = len(serverAccts)
		details["serverAddrs"] = strings.Join(serverAddrs, ",")
	} else {
		details["error"] = err.Error()
	}

	assert.Unreachable("singleton_driver_model: asset-index account query outside model", details)
}

// --- index-op generation -------------------------------------------------

// createIndexReq / dropIndexReq wrap an IndexID as a CreateIndex / DropIndex
// Apply request. Both are idempotent on the server (a duplicate create on a
// present index is a no-op, a drop of an absent index a no-op — no
// AlreadyExists / NotFound), so the model applies them as always-OK.
func createIndexReq(ledger string, id *commonpb.IndexID) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreateIndex{
		CreateIndex: &servicepb.CreateIndexRequest{Ledger: ledger, Id: id},
	}}
}

func dropIndexReq(ledger string, id *commonpb.IndexID) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_DropIndex{
		DropIndex: &servicepb.DropIndexRequest{Ledger: ledger, Id: id},
	}}
}

// rollIndexOp: ~1-in-16 a bulk is an index create/drop rather than ledger
// traffic, churning the index lifecycles the indexed queries probe.
func rollIndexOp() bool {
	return oneIn(16)
}

// generateIndexOp picks one workload index; creates it when the ledger lacks
// it, else occasionally drops it (so the lifecycle keeps cycling) and otherwise
// leaves it in place for queries to validate against. One-in-16 it instead
// probes CreateIndex on an UNDECLARED metadata field — rejected with
// METADATA_FIELD_NOT_IN_SCHEMA, which the model predicts identically. Reads
// committed state only.
func generateIndexOp(g oracle.GlobalState, ledger string) *servicepb.Request {
	if oneIn(16) {
		return createIndexReq(ledger, indexes.MetadataID(
			commonpb.TargetType_TARGET_TYPE_ACCOUNT, "undeclared-"+metaKey()))
	}

	ls := g.Ledger(ledger)
	all := workloadIndexes()

	// The declared metadata fields of both indexable targets join the pool, so
	// metadata index lifecycles churn alongside the builtins.
	for _, target := range []commonpb.TargetType{
		commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		commonpb.TargetType_TARGET_TYPE_TRANSACTION,
	} {
		for key := range ls.FieldTypesFor(target).All() {
			id := indexes.MetadataID(target, key)
			all = append(all, workloadIndex{id, indexes.Canonical(id)})
		}
	}

	pick := all[internal.Rand().Intn(len(all))]

	exists, _ := ls.IndexState(pick.canonical)
	if !exists {
		return createIndexReq(ledger, pick.id)
	}

	if oneIn(3) {
		return dropIndexReq(ledger, pick.id)
	}

	return nil
}

// --- readiness poller ----------------------------------------------------

// trackedIndexes snapshots the (ledger → canonical IDs) set the model currently
// holds, so the poller can query each one's per-replica readiness without holding
// mu across the network round-trips.
func (c *Checker) trackedIndexes() map[string][]string {
	c.mu.Lock()
	defer c.mu.Unlock()

	out := map[string][]string{}
	for _, ledger := range c.ledgerNames {
		for canon := range c.modelState.Ledger(ledger).Indexes().All() {
			out[ledger] = append(out[ledger], canon)
		}
	}

	return out
}

// demoteAllIndexes flips every tracked index back to ambiguous. Called around a
// restore cycle: the restored node rebuilds its read-store from the log, so its
// indexes re-enter BUILDING (CurrentVersion 0) until the backfill catches up —
// the model must tolerate a not-ready rejection again until the poller reconfirms
// readiness. Demotion only widens tolerance, so it can never cause a finding.
func (c *Checker) demoteAllIndexes() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, ledger := range c.ledgerNames {
		for canon := range c.modelState.Ledger(ledger).Indexes().All() {
			c.modelState.SetIndexAmbiguous(ledger, canon)
		}
	}
}

// runIndexReadinessPoller reconciles each tracked index's active flag against the
// cluster on a jittered interval until ctx ends. Because queries hit any replica
// through the round-robin client, an index is active only when every reachable
// replica reports CurrentVersion>0 for it; a single not-ready (or unreachable)
// replica demotes it to ambiguous, where a not-ready rejection is tolerated.
func runIndexReadinessPoller(ctx context.Context, c *Checker, conns internal.PerNodeConns, interval time.Duration) {
	for {
		jitter := time.Duration(internal.Rand().Int63n(int64(interval/2) + 1))
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval + jitter):
		}

		reconcileIndexes(ctx, c, conns)
	}
}

// reconcileIndexes polls every replica's GetIndexStatus for each ledger holding a
// tracked index and promotes/demotes each index by whether every replica reports
// it live. A replica that is unreachable this tick counts as not-ready, so a
// transient node down demotes rather than freezing a stale active flag.
func reconcileIndexes(ctx context.Context, c *Checker, conns internal.PerNodeConns) {
	for ledger, canons := range c.trackedIndexes() {
		// readyAll starts true and is cleared for any canonical a replica reports
		// missing, not-yet-live, or that we could not read at all.
		readyAll := make(map[string]bool, len(canons))
		for _, canon := range canons {
			readyAll[canon] = true
		}

		for _, pc := range conns {
			resp, err := pc.Bucket.GetIndexStatus(ctx, &servicepb.GetIndexStatusRequest{Ledger: ledger})
			if err != nil {
				for _, canon := range canons {
					readyAll[canon] = false
				}

				continue
			}

			version := make(map[string]uint32, len(resp.GetIndexes()))
			for _, e := range resp.GetIndexes() {
				version[indexes.Canonical(e.GetIndex().GetId())] = e.GetCurrentVersion()
			}

			for _, canon := range canons {
				if version[canon] == 0 { // absent on this replica, or backfill not yet live
					readyAll[canon] = false
				}
			}
		}

		c.mu.Lock()
		for _, canon := range canons {
			exists, wasActive := c.modelState.Ledger(ledger).IndexState(canon)
			if !exists {
				continue // dropped between the snapshot and now
			}

			if readyAll[canon] {
				c.modelState.SetIndexActive(ledger, canon)
				if !wasActive {
					// Coverage: the poller confirmed the index READY on every replica
					// and promoted it — after which its queries must return results.
					assert.Reachable("singleton_driver_model: index promoted to active", internal.Details{"ledger": ledger, "index": canon})
				}
			} else {
				c.modelState.SetIndexAmbiguous(ledger, canon)
				if wasActive {
					// Coverage: a replica reported the index not-ready again (node
					// down / restored node rebuilding), demoting it back to ambiguous.
					assert.Reachable("singleton_driver_model: index demoted to ambiguous", internal.Details{"ledger": ledger, "index": canon})
				}
			}
		}
		c.mu.Unlock()
	}
}

// --- indexed transaction-query validation ----------------------------------

// validateIndexedTransactionQuery validates a ListTransactions query whose
// filter carries index-backed tx leaves. needed holds the canonical IndexIDs
// the compiler must find READY. The legal outcomes per candidate base:
//
//   - a result set is legal iff the base holds every needed index (ambiguous or
//     active) and the page is a legal window over the base's rows
//     (txWindowMatches);
//   - a not-ready rejection (FailedPrecondition) is legal iff some needed index
//     is not active on the base (absent, ambiguous, or never built).
//
// Any other error code is a finding, as are rows without every needed index and
// a rejection when every needed index is active on every base.
func (c *Checker) validateIndexedTransactionQuery(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, needed map[string]struct{}, afterID uint64, pageSize int, reverse bool, serverTxs []*commonpb.Transaction, err error) {
	errKind, ok := classifyIndexedQueryError(err)
	if !ok {
		assert.Unreachable("singleton_driver_model: indexed transaction query returned unexpected error", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	if c.matchesModel(maxTicket, "TXQUERY-IDX", func(cand oracle.GlobalState) bool {
		return indexedQueryOutcomeLegal(cand.Ledger(ledger), commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, filter, needed, errKind, func(ls oracle.LedgerState) bool {
			return txWindowMatches(ls, filter, afterID, pageSize, reverse, serverTxs)
		})
	}) {
		switch errKind {
		case indexedErrNone:
			assert.Reachable("singleton_driver_model: indexed transaction query served results", internal.Details{"ledger": ledger})
		case indexedErrCompilation:
			assert.Reachable("singleton_driver_model: kind-mismatched field query rejected", internal.Details{"ledger": ledger})
		default:
			assert.Reachable("singleton_driver_model: indexed transaction query gated", internal.Details{"ledger": ledger})
		}

		return
	}

	c.mu.Lock()
	modelLS := c.modelState.Ledger(ledger)
	idxStates := make([]string, 0, len(needed))
	for canon := range needed {
		exists, active := modelLS.IndexState(canon)
		idxStates = append(idxStates, canon+"="+indexStateLabel(exists, active))
	}
	modelWindow := transactionWindow(modelLS, filter, afterID, pageSize, reverse)
	c.mu.Unlock()

	details := internal.Details{
		"ledger":   ledger,
		"filter":   describeFilter(filter),
		"afterId":  afterID,
		"pageSize": pageSize,
		"reverse":  reverse,
		"modelIdx": strings.Join(idxStates, " "),
		"modelIds": joinUint64(modelWindow),
		"bases": c.describeCandidateVerdicts(maxTicket, ledger, needed, func(ls oracle.LedgerState) []string {
			ids := transactionWindow(ls, filter, afterID, pageSize, reverse)
			out := make([]string, len(ids))
			for i, id := range ids {
				out[i] = strconv.FormatUint(id, 10)
			}

			return out
		}),
	}
	if err == nil {
		serverIds := make([]uint64, len(serverTxs))
		for i, t := range serverTxs {
			serverIds[i] = t.GetId()
		}
		details["rows"] = len(serverTxs)
		details["serverIds"] = joinUint64(serverIds)
	} else {
		details["error"] = err.Error()
	}

	if err == nil {
		inModel := map[uint64]bool{}
		for _, id := range modelWindow {
			inModel[id] = true
		}

		var surplus []string
		for _, t := range serverTxs {
			if id := t.GetId(); !inModel[id] {
				surplus = append(surplus, strconv.FormatUint(id, 10)+"{"+c.modelTxMetaDump(ledger, id)+"}")
			}
		}

		details["surplusModelMeta"] = strings.Join(surplus, " ; ")
	}

	assert.Unreachable("singleton_driver_model: indexed transaction query outside model", details)
}

// describeCandidateVerdicts re-enumerates a failed observation's candidate
// bases and renders, per base (capped), the needed-index states and the model
// window head — the context a finding needs to distinguish a server-side
// divergence (no base explains the response) from an envelope gap (the
// explaining base was never enumerated). Caller holds c.mu.
func (c *Checker) describeCandidateVerdicts(maxTicket uint64, ledger string, needed map[string]struct{}, window func(oracle.LedgerState) []string) string {
	const maxBases = 8

	var parts []string

	n := 0
	c.candidateBases(maxTicket, func(cand oracle.GlobalState) bool {
		n++
		if n > maxBases {
			return false
		}

		ls := cand.Ledger(ledger)
		idx := make([]string, 0, len(needed))
		for canon := range needed {
			exists, active := ls.IndexState(canon)
			idx = append(idx, canon+"="+indexStateLabel(exists, active))
		}
		sort.Strings(idx)

		w := window(ls)
		if len(w) > 6 {
			w = append(w[:6:6], "…")
		}

		parts = append(parts, fmt.Sprintf("b%d{%s|w=%s}", n, strings.Join(idx, " "), strings.Join(w, ",")))

		return false
	})

	return fmt.Sprintf("%d bases: %s", n, strings.Join(parts, " ; "))
}

// metadataCanonical is the canonical IndexID of the per-(target, key) metadata
// index serving Field conditions on the given query target.
func metadataCanonical(target commonpb.QueryTarget, key string) string {
	tt := commonpb.TargetType_TARGET_TYPE_ACCOUNT
	if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
		tt = commonpb.TargetType_TARGET_TYPE_TRANSACTION
	}

	return indexes.Canonical(indexes.MetadataID(tt, key))
}

// indexedQueryOutcomeLegal is the shared per-candidate verdict for a query
// whose filter needs indexes. Per Field leaf the compiler checks
// schema → requireIndexReady → kind coercion, but across a multi-leaf tree
// the surfaced error follows the compiler's walk order, which the model does
// not pin down. So:
//
//   - a FailedPrecondition (index not found / not ready) is legal iff some
//     needed index is not active on the base;
//   - a FILTER_COMPILATION rejection (InvalidArgument) is legal iff the filter
//     carries a kind-mismatched Field leaf under the base's declared types —
//     regardless of the other leaves' index states: when a mismatched leaf and
//     an absent index coexist (e.g. or(field:absent, field:mismatched)),
//     whichever leaf the walk reaches first decides which honest rejection
//     surfaces, and both are legal;
//   - results are legal iff there is no kind mismatch, every needed index
//     exists, and the window matches (windowMatches).
func indexedQueryOutcomeLegal(
	ls oracle.LedgerState,
	target commonpb.QueryTarget,
	filter *commonpb.QueryFilter,
	needed map[string]struct{},
	errKind indexedErrKind,
	windowMatches func(oracle.LedgerState) bool,
) bool {
	switch errKind {
	case indexedErrNotReady:
		for canon := range needed {
			if exists, active := ls.IndexState(canon); !exists || !active {
				return true
			}
		}

		return false
	case indexedErrCompilation:
		return fieldKindMismatch(ls, filter, target)
	default: // results
		if fieldKindMismatch(ls, filter, target) {
			return false
		}
		for canon := range needed {
			if exists, _ := ls.IndexState(canon); !exists {
				return false
			}
		}

		return windowMatches(ls)
	}
}

// indexedErrKind classifies the observed outcome of an index-backed query.
type indexedErrKind int

const (
	indexedErrNone indexedErrKind = iota
	indexedErrNotReady
	indexedErrCompilation
)

// classifyIndexedQueryError buckets err for indexedQueryOutcomeLegal; ok=false
// means the code is not part of the indexed-query surface at all (a finding).
func classifyIndexedQueryError(err error) (indexedErrKind, bool) {
	switch {
	case err == nil:
		return indexedErrNone, true
	case status.Code(err) == codes.FailedPrecondition:
		return indexedErrNotReady, true
	case status.Code(err) == codes.InvalidArgument && internal.HasErrorReason(err, "FILTER_COMPILATION_ERROR"):
		return indexedErrCompilation, true
	default:
		return indexedErrNone, false
	}
}

// validateIndexedAccountQuery is the accounts twin of
// validateIndexedTransactionQuery: same needed-set lifecycle gating, with the
// ordered account window (accountWindow + accountMatches) as the result check.
func (c *Checker) validateIndexedAccountQuery(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, needed map[string]struct{}, cursor string, pageSize int, reverse bool, serverAccts []*commonpb.Account, err error) {
	errKind, ok := classifyIndexedQueryError(err)
	if !ok {
		assert.Unreachable("singleton_driver_model: indexed account query returned unexpected error", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"error":  err.Error(),
		})

		return
	}

	if c.matchesModel(maxTicket, "AQUERY-IDX", func(cand oracle.GlobalState) bool {
		return indexedQueryOutcomeLegal(cand.Ledger(ledger), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, filter, needed, errKind, func(ls oracle.LedgerState) bool {
			want := accountWindow(ls, filter, cursor, pageSize, reverse)
			if len(want) != len(serverAccts) {
				return false
			}

			for i, addr := range want {
				if serverAccts[i].GetAddress() != addr || !accountMatches(ls, addr, serverAccts[i]) {
					return false
				}
			}

			return true
		})
	}) {
		switch errKind {
		case indexedErrNone:
			assert.Reachable("singleton_driver_model: indexed account query served results", internal.Details{"ledger": ledger})
		case indexedErrCompilation:
			assert.Reachable("singleton_driver_model: kind-mismatched field query rejected", internal.Details{"ledger": ledger})
		default:
			assert.Reachable("singleton_driver_model: indexed account query gated", internal.Details{"ledger": ledger})
		}

		return
	}

	c.mu.Lock()
	modelLS := c.modelState.Ledger(ledger)
	idxStates := make([]string, 0, len(needed))
	for canon := range needed {
		exists, active := modelLS.IndexState(canon)
		idxStates = append(idxStates, canon+"="+indexStateLabel(exists, active))
	}
	modelWindow := accountWindow(modelLS, filter, cursor, pageSize, reverse)
	c.mu.Unlock()

	details := internal.Details{
		"ledger":     ledger,
		"filter":     describeFilter(filter),
		"cursor":     cursor,
		"pageSize":   pageSize,
		"reverse":    reverse,
		"modelIdx":   strings.Join(idxStates, " "),
		"modelAddrs": strings.Join(modelWindow, ","),
		"bases": c.describeCandidateVerdicts(maxTicket, ledger, needed, func(ls oracle.LedgerState) []string {
			return accountWindow(ls, filter, cursor, pageSize, reverse)
		}),
	}
	if err == nil {
		serverAddrs := make([]string, len(serverAccts))
		for i, a := range serverAccts {
			serverAddrs[i] = a.GetAddress()
		}
		details["rows"] = len(serverAccts)
		details["serverAddrs"] = strings.Join(serverAddrs, ",")
	} else {
		details["error"] = err.Error()
	}

	if err == nil {
		inModel := map[string]bool{}
		for _, a := range modelWindow {
			inModel[a] = true
		}

		var surplus []string
		for _, a := range serverAccts {
			if addr := a.GetAddress(); !inModel[addr] {
				surplus = append(surplus, addr+"{"+c.modelAccountMetaDump(ledger, addr)+"}")
			}
		}

		details["surplusModelMeta"] = strings.Join(surplus, " ; ")
	}

	assert.Unreachable("singleton_driver_model: indexed account query outside model", details)
}
