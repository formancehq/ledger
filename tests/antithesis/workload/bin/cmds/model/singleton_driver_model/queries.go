package main

import (
	"context"
	"io"
	"sort"
	"strconv"
	"strings"

	"github.com/antithesishq/antithesis-sdk-go/assert"
	"github.com/antithesishq/antithesis-sdk-go/random"
	"github.com/holiman/uint256"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
)

// Query reads exercise ListAccounts / ListTransactions — the filtered,
// paginated, ordered read surface. A list page is a deterministic ordered
// window: given the filter, the sort order, the cursor, the page size, and the
// reverse flag, exactly one slice of the matching entities is correct. The
// model reproduces that slice over a candidate base and checks the streamed
// page element-for-element, so both membership and order are validated.
//
// Ordering (server): accounts by address bytes, transactions by id, ascending;
// reverse flips it. The cursor is exclusive in both directions — forward drops
// keys <= cursor, reverse drops keys >= cursor (PaginateForward / listDescFiltered).
//
// Index gating: the server's filter compiler serves most conditions from a
// created index and rejects the query when the index is absent or not yet
// ready. The generator churns the full index lifecycle (indexes.go), so every
// index-backed filter routes through the per-filter needed-set validation
// (neededIndexCanonicals + validateIndexedAccountQuery /
// validateIndexedTransactionQuery): results require every needed index and an
// exact window, a not-ready rejection is legal only while some needed index is
// not active, and a kind-mismatched Field leaf must be rejected as a
// compilation error. The index-free conditions — universe,
// address-on-accounts, reverted, the tx-id builtin, and boolean compositions
// of these — always return the window the model computes.

// queryPageSize is the page size a query read requests. Kept well under
// MaxPageSize (1000) so the server never clamps it — the model uses the same
// value to size its window, and a clamp would desync the two.
func queryPageSize() int {
	return int(random.RandomChoice([]uint8{1, 2, 3, 5, 10, 50}))
}

// runAccountQuery issues a linearizable ListAccounts and checks the streamed
// page against the model's ordered window (see validateAccountQuery). One-in-six
// queries carry an index-backed filter to exercise the NotFound rejection path.
func runAccountQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger := random.RandomChoice(c.ledgerNames)
	filter := genAccountFilter(c.sampleAccountFieldSeeds(ledger))
	needed := map[string]struct{}{}
	neededIndexCanonicals(filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS, needed)
	_, _, bareAsset := hasAssetTarget(filter)
	invalidTarget := filterInvalidForTarget(filter, commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
	pageSize := queryPageSize()
	reverse := random.RandomChoice([]uint8{0, 1}) == 1

	var cursor string
	if random.RandomChoice([]uint8{0, 1}) == 0 {
		cursor = poolAddress()
	}

	c.mu.Lock()
	readID := c.registerRead()
	minSeq := c.observedFrontier()
	c.mu.Unlock()
	defer c.finishRead(readID)

	// Pin the read to the observed frontier: the server snapshot is then at
	// least every state the drain gate may fold into modelState while this
	// read is in flight, so the ordered window stays representable by a
	// candidate base (see observedFrontier).
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	stream, err := client.ListAccounts(readCtx, &servicepb.ListAccountsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: uint32(pageSize),
			Cursor:   cursor,
			Reverse:  reverse,
			Filter:   filter,
			Read:     &commonpb.ReadOptions{MinLogSequence: minSeq},
		},
	})

	var accounts []*commonpb.Account
	if err == nil {
		accounts, err = drainStream(stream)
	}

	diag := streamDiag(stream)

	// High-water at the read's completion: only bulks dispatched by now could be
	// reflected in the page. Captured before validation so later dispatches
	// aren't folded into this read's candidate states.
	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if handleInvalidTargetError(invalidTarget, "account", ledger, filter, err) {
			return
		}
		if bareAsset {
			// The account-by-asset index governs this filter's outcome; a not-ready
			// error is legal while the index is absent or ambiguous.
			c.validateAssetAccountQuery(maxTicket, ledger, filter, cursor, pageSize, reverse, nil, err, diag)
			return
		}
		if len(needed) > 0 {
			c.validateIndexedAccountQuery(maxTicket, ledger, filter, needed, cursor, pageSize, reverse, nil, err, diag)
			return
		}

		assert.Unreachable("singleton_driver_model: ListAccounts returned unexpected error", internal.Details{
			"ledger":     ledger,
			"filter":     describeFilter(filter),
			"error":      err.Error(),
			"serverDiag": diag,
		})

		return
	}

	if invalidTarget {
		// The filter carries a condition invalid on this target; the server must
		// reject it, not stream rows.
		assert.Unreachable("singleton_driver_model: target-invalid account query returned results", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"rows":   len(accounts),
		})

		return
	}

	if bareAsset {
		c.validateAssetAccountQuery(maxTicket, ledger, filter, cursor, pageSize, reverse, accounts, nil, diag)
		return
	}

	if len(needed) > 0 {
		c.validateIndexedAccountQuery(maxTicket, ledger, filter, needed, cursor, pageSize, reverse, accounts, nil, diag)
		return
	}

	c.validateAccountQuery(maxTicket, ledger, filter, cursor, pageSize, reverse, accounts, diag)
}

// sampleAccountFieldSeeds snapshots the ledger's declared account fields plus
// value samples for the filter generator. Acquires c.mu.
func (c *Checker) sampleAccountFieldSeeds(ledger string) []fieldSeed {
	c.mu.Lock()
	defer c.mu.Unlock()

	return sampleFieldSeeds(c.modelState.Ledger(ledger), commonpb.QueryTarget_QUERY_TARGET_ACCOUNTS)
}

// runTransactionQuery issues a linearizable ListTransactions and checks the
// streamed page against the model's ordered window (see validateTransactionQuery).
// One-in-six queries carry an index-backed filter to exercise the NotFound path.
func runTransactionQuery(ctx context.Context, client servicepb.BucketServiceClient, c *Checker) {
	ledger := random.RandomChoice(c.ledgerNames)
	filter := genTransactionFilter(c.sampleTxFilterSeeds(ledger))
	needed := map[string]struct{}{}
	neededIndexCanonicals(filter, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS, needed)
	invalidTarget := filterInvalidForTarget(filter, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)
	pageSize := queryPageSize()
	reverse := random.RandomChoice([]uint8{0, 1}) == 1

	var (
		cursor  string
		afterID uint64
	)
	if random.RandomChoice([]uint8{0, 1}) == 0 {
		afterID = 1 + internal.Rand().Uint64()%256
		cursor = strconv.FormatUint(afterID, 10)
	}

	c.mu.Lock()
	readID := c.registerRead()
	minSeq := c.observedFrontier()
	c.mu.Unlock()
	defer c.finishRead(readID)

	// Pin the read to the observed frontier: the server snapshot is then at
	// least every state the drain gate may fold into modelState while this
	// read is in flight, so the ordered window stays representable by a
	// candidate base (see observedFrontier).
	readCtx := metadata.AppendToOutgoingContext(ctx, "x-consistency", "linearizable")
	stream, err := client.ListTransactions(readCtx, &servicepb.ListTransactionsRequest{
		Ledger: ledger,
		Options: &commonpb.ListOptions{
			PageSize: uint32(pageSize),
			Cursor:   cursor,
			Reverse:  reverse,
			Filter:   filter,
			Read:     &commonpb.ReadOptions{MinLogSequence: minSeq},
		},
	})

	var txs []*commonpb.Transaction
	if err == nil {
		txs, err = drainStream(stream)
	}

	diag := streamDiag(stream)

	maxTicket := c.ticketSeq.Load()

	if err != nil {
		if internal.IsTransient(err) || isShutdownError(err) {
			return
		}
		if handleInvalidTargetError(invalidTarget, "transaction", ledger, filter, err) {
			return
		}
		if len(needed) > 0 {
			c.validateIndexedTransactionQuery(maxTicket, ledger, filter, needed, afterID, pageSize, reverse, nil, err, diag)
			return
		}

		assert.Unreachable("singleton_driver_model: ListTransactions returned unexpected error", internal.Details{
			"ledger":     ledger,
			"filter":     describeFilter(filter),
			"error":      err.Error(),
			"serverDiag": diag,
		})

		return
	}

	if invalidTarget {
		assert.Unreachable("singleton_driver_model: target-invalid transaction query returned results", internal.Details{
			"ledger": ledger,
			"filter": describeFilter(filter),
			"rows":   len(txs),
		})

		return
	}

	if len(needed) > 0 {
		c.validateIndexedTransactionQuery(maxTicket, ledger, filter, needed, afterID, pageSize, reverse, txs, nil, diag)
		return
	}

	c.validateTransactionQuery(maxTicket, ledger, filter, afterID, pageSize, reverse, txs, diag)
}

// handleInvalidTargetError validates the error of a filter carrying a condition
// invalid on its target (e.g. reverted on accounts, account-has-asset on
// transactions). The server's rejectInvalidCondition runs before index checks,
// so this must be consulted before handleIndexGatedError; the expected outcome
// is InvalidArgument. Returns true when it has fully handled err.
func handleInvalidTargetError(invalidTarget bool, kind, ledger string, filter *commonpb.QueryFilter, err error) bool {
	if !invalidTarget {
		return false
	}

	if status.Code(err) == codes.InvalidArgument {
		// Coverage: the per-target validity rejection is actually exercised.
		assert.Reachable("singleton_driver_model: target-invalid query rejected", internal.Details{"kind": kind})

		return true
	}

	assert.Unreachable("singleton_driver_model: target-invalid query returned unexpected error", internal.Details{
		"kind":   kind,
		"ledger": ledger,
		"filter": describeFilter(filter),
		"error":  err.Error(),
	})

	return true
}

// drainStream reads a server stream to exhaustion, returning every item in
// stream order. io.EOF is clean end-of-stream; any other error (a compile
// rejection surfaces on the first Recv) is returned alongside what was read.
func drainStream[T any](stream grpc.ServerStreamingClient[T]) ([]*T, error) {
	var out []*T
	for {
		item, err := stream.Recv()
		if err == io.EOF {
			return out, nil
		}
		if err != nil {
			return out, err
		}

		out = append(out, item)
	}
}

// validateAccountQuery checks a ListAccounts page against the model: legal iff
// some candidate base's ordered window — filtered, sorted, cursor-skipped,
// page-capped — equals the streamed accounts position-for-position, each row's
// address AND its whole volumes/metadata snapshot matching on that same base.
func (c *Checker) validateAccountQuery(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, cursor string, pageSize int, reverse bool, serverAccts []*commonpb.Account, diag ...string) {
	if c.matchesModel(maxTicket, "AQUERY", func(base oracle.GlobalState) bool {
		ls := base.Ledger(ledger)
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
	}) {
		return
	}

	serverAddrs := make([]string, len(serverAccts))
	for i, a := range serverAccts {
		serverAddrs[i] = a.GetAddress()
	}

	assert.Unreachable("singleton_driver_model: account query outside model", internal.Details{
		"serverDiag":  firstDiag(diag),
		"ledger":      ledger,
		"filter":      describeFilter(filter),
		"cursor":      cursor,
		"pageSize":    pageSize,
		"reverse":     reverse,
		"rows":        len(serverAccts),
		"serverAddrs": strings.Join(serverAddrs, ","),
		"modelAddrs":  strings.Join(c.modelAccountWindow(ledger, filter, cursor, pageSize, reverse), ","),
	})
}

// modelAccountWindow returns the account window on the committed modelState — the
// base with no in-flight bulks folded — for a finding's diagnostics. Acquires c.mu.
func (c *Checker) modelAccountWindow(ledger string, filter *commonpb.QueryFilter, cursor string, pageSize int, reverse bool) []string {
	c.mu.Lock()
	defer c.mu.Unlock()

	return accountWindow(c.modelState.Ledger(ledger), filter, cursor, pageSize, reverse)
}

// validateTransactionQuery checks a ListTransactions page against the model:
// legal iff some candidate base's ordered window equals the streamed
// transactions position-for-position, each row matching the model record at its
// id (see txRecordMatches) on that same base.
func (c *Checker) validateTransactionQuery(maxTicket uint64, ledger string, filter *commonpb.QueryFilter, afterID uint64, pageSize int, reverse bool, serverTxs []*commonpb.Transaction, diag ...string) {
	if c.matchesModel(maxTicket, "TXQUERY", func(base oracle.GlobalState) bool {
		return txWindowMatches(base.Ledger(ledger), filter, afterID, pageSize, reverse, serverTxs)
	}) {
		return
	}

	serverIds := make([]uint64, len(serverTxs))
	for i, t := range serverTxs {
		serverIds[i] = t.GetId()
	}

	assert.Unreachable("singleton_driver_model: transaction query outside model", internal.Details{
		"serverDiag": firstDiag(diag),
		"ledger":     ledger,
		"filter":     describeFilter(filter),
		"afterId":    afterID,
		"pageSize":   pageSize,
		"reverse":    reverse,
		"rows":       len(serverTxs),
		"serverIds":  joinUint64(serverIds),
		"modelIds":   joinUint64(c.modelTransactionWindow(ledger, filter, afterID, pageSize, reverse)),
	})
}

// modelTransactionWindow returns the transaction window on the committed
// modelState — the base with no in-flight bulks folded — for a finding's
// diagnostics. Acquires c.mu.
func (c *Checker) modelTransactionWindow(ledger string, filter *commonpb.QueryFilter, afterID uint64, pageSize int, reverse bool) []uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	return transactionWindow(c.modelState.Ledger(ledger), filter, afterID, pageSize, reverse)
}

// accountWindow is the model's prediction of a ListAccounts page: the ledger's
// accounts matching filter, in address order (reversed when reverse), with the
// exclusive cursor applied and capped at pageSize. Because the list is sorted,
// dropping every key past the cursor equals the server's contiguous prefix skip.
func accountWindow(ls oracle.LedgerState, filter *commonpb.QueryFilter, cursor string, pageSize int, reverse bool) []string {
	var window []string
	for _, addr := range accountUniverse(ls) {
		if matchAccountFilter(ls, filter, addr) {
			window = append(window, addr)
		}
	}

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

// transactionWindow is the required-row window — the ids a page must show when
// every filter verdict is known — capped at pageSize. Used for a finding's
// diagnostics; validation goes through txWindowMatches, which also handles
// optional rows.
func transactionWindow(ls oracle.LedgerState, filter *commonpb.QueryFilter, afterID uint64, pageSize int, reverse bool) []uint64 {
	var window []uint64
	for _, row := range transactionWindowRows(ls, filter, afterID, reverse) {
		if row.required {
			window = append(window, row.id)
		}
	}

	if len(window) > pageSize {
		window = window[:pageSize]
	}

	return window
}

// txWindowRow is one ordered candidate row of a predicted transaction page:
// required rows must appear in the server page, optional ones (a filter verdict
// hinging on a not-yet-learned server stamp) may.
type txWindowRow struct {
	id       uint64
	required bool
}

// transactionWindowRows is the ordered, cursor-filtered, UNTRUNCATED row
// sequence a ListTransactions page draws from. Truncation is the matcher's job
// (txWindowMatches) — optional rows may or may not consume page slots, so a
// fixed prefix cut would be wrong.
//
// The transactions endpoint INVERTS the API reverse flag (controller
// ListTransactionsFrom passes reverse=!reverse to listEntities): reverse=false
// yields newest-first (descending id), reverse=true yields oldest-first
// (ascending). This is the opposite of accounts, which follow reverse literally.
// Descending pages drop ids >= afterID (older-than cursor); ascending pages drop
// ids <= afterID (newer-than cursor) — mirroring PaginateReverse / PaginateForward.
func transactionWindowRows(ls oracle.LedgerState, filter *commonpb.QueryFilter, afterID uint64, reverse bool) []txWindowRow {
	descending := !reverse

	var rows []txWindowRow
	for _, rec := range ls.Txs().All() {
		match, known := matchTxFilter(ls, filter, rec)
		if known && !match {
			continue
		}

		rows = append(rows, txWindowRow{id: rec.Id(), required: known})
	}

	if descending {
		for i, j := 0, len(rows)-1; i < j; i, j = i+1, j-1 {
			rows[i], rows[j] = rows[j], rows[i]
		}
	}

	if afterID != 0 {
		kept := rows[:0]
		for _, row := range rows {
			if descending && row.id >= afterID || !descending && row.id <= afterID {
				continue
			}

			kept = append(kept, row)
		}
		rows = kept
	}

	return rows
}

// txWindowMatches reports whether the server page is exactly a legal window
// over the candidate's row sequence: required rows appear in order (each
// content-matching its model record), optional rows may, nothing else does, and
// a required row may only be missing past a full (truncated) page.
func txWindowMatches(ls oracle.LedgerState, filter *commonpb.QueryFilter, afterID uint64, pageSize int, reverse bool, serverTxs []*commonpb.Transaction) bool {
	if len(serverTxs) > pageSize {
		return false
	}

	txs := ls.Txs()
	j := 0

	for _, row := range transactionWindowRows(ls, filter, afterID, reverse) {
		if j == len(serverTxs) {
			if len(serverTxs) == pageSize {
				return true // full page — the remaining rows were truncated
			}
			if row.required {
				return false // page had room, yet a required row is missing
			}

			continue
		}

		if serverTxs[j].GetId() == row.id {
			if !txRecordMatches(txs.Get(int(row.id-1)), serverTxs[j]) {
				return false
			}
			j++
		} else if row.required {
			return false
		}
	}

	return j == len(serverTxs)
}

// accountUniverse returns ls's account addresses — every address carrying a
// volume cell or a metadata entry — in ascending byte order, matching the
// server's merged V+M attribute scan (readstore.NewPebbleAccountIterator).
func accountUniverse(ls oracle.LedgerState) []string {
	seen := map[string]struct{}{}
	for k := range ls.Volumes().All() {
		seen[k.Address] = struct{}{}
	}
	for k := range ls.Metadata().All() {
		seen[k.Address] = struct{}{}
	}

	out := make([]string, 0, len(seen))
	for addr := range seen {
		out = append(out, addr)
	}

	sort.Strings(out)

	return out
}

// accountMatches reports whether the server account is exactly what the model
// holds for addr: the same uncolored volume cells (per asset, input and output)
// and the same metadata. The workload only exercises uncolored postings, so
// colored buckets are out of scope. metadataMatches is shared with the single
// GetAccount read.
func accountMatches(ls oracle.LedgerState, addr string, serverAcct *commonpb.Account) bool {
	if !metadataMatches(ls, addr, serverAcct.GetMetadata()) {
		return false
	}

	model := map[string]oracle.VolumePair{}
	for k, vp := range ls.Volumes().All() {
		if k.Address == addr {
			model[k.Asset] = vp
		}
	}

	server := map[string]struct{ in, out uint256.Int }{}
	for _, av := range serverAcct.GetVolumes() {
		if av.GetColor() != "" {
			continue
		}

		var in, out uint256.Int
		if err := in.SetFromDecimal(av.GetVolumes().GetInput()); err != nil {
			return false
		}
		if err := out.SetFromDecimal(av.GetVolumes().GetOutput()); err != nil {
			return false
		}

		server[av.GetAsset()] = struct{ in, out uint256.Int }{in, out}
	}

	if len(model) != len(server) {
		return false
	}

	for asset, vp := range model {
		sv, ok := server[asset]
		if !ok || vp.Input.Cmp(&sv.in) != 0 || vp.Output.Cmp(&sv.out) != 0 {
			return false
		}
	}

	return true
}

// txRecordView is the subset of oracle's (unexported) transaction record that
// the query and single-read validators compare against. Declaring the interface
// here lets both share txRecordMatches without oracle exporting the concrete type.
type txRecordView interface {
	Id() uint64
	Reference() string
	Postings() []*commonpb.Posting
	Metadata() map[string]*commonpb.MetadataValue
	Reverted() bool
	Timestamp() *commonpb.Timestamp
	InsertedAt() *commonpb.Timestamp
	RevertedBy() uint64
	RevertedAt() *commonpb.Timestamp
	RevertsTransaction() uint64
	IndexedAddrs() map[string]uint8
}

// txRecordMatches reports whether the model record rec is consistent with the
// server transaction: same id, reference, revert relationships, postings, and
// metadata. Timestamps follow the model's nil-means-server-dated convention — a
// nil model timestamp is unpredictable, so it is not checked; reverted_at is the
// same, and only a reverted record may carry one at all.
func txRecordMatches(rec txRecordView, serverTx *commonpb.Transaction) bool {
	tsOK := rec.Timestamp() == nil || rec.Timestamp().GetData() == serverTx.GetTimestamp().GetData()

	var raOK bool
	switch {
	case rec.RevertedAt() != nil:
		raOK = serverTx.GetRevertedAt() != nil && rec.RevertedAt().GetData() == serverTx.GetRevertedAt().GetData()
	case rec.Reverted():
		raOK = true
	default:
		raOK = serverTx.GetRevertedAt() == nil
	}

	return rec.Id() == serverTx.GetId() &&
		rec.Reference() == serverTx.GetReference() &&
		rec.Reverted() == serverTx.GetReverted() &&
		rec.RevertedBy() == serverTx.GetRevertedByTransaction() &&
		rec.RevertsTransaction() == serverTx.GetRevertsTransaction() &&
		tsOK && raOK &&
		postingsEqual(rec.Postings(), serverTx.GetPostings()) &&
		metaMapEqual(rec.Metadata(), serverTx.GetMetadata())
}

// --- Filter generation --------------------------------------------------

// maxQueryGenDepth bounds generated boolean nesting. It must stay well under
// domain.MaxFilterDepth (100), the depth the compiler rejects.
const maxQueryGenDepth = 3

// oneIn reports a 1-in-n chance through the Antithesis chooser, so the platform
// can steer toward the (rare) branch it gates. n must be in [1, 256].
func oneIn(n int) bool {
	choices := make([]uint8, n)
	for i := range choices {
		choices[i] = uint8(i)
	}

	return random.RandomChoice(choices) == 0
}

// genAccountFilter rolls a query filter for ListAccounts. One-in-six is an
// index-backed metadata condition the driver never creates (the gated / NotFound
// path); one-in-six is an account-by-asset filter whose outcome the index
// lifecycle governs (see genAccountAssetFilter); ~1-in-16 is a transactions-only
// condition invalid on this target (the InvalidArgument path); then one-in-four
// is the no-filter universe (a nil top-level filter); the rest are index-free
// address filters and boolean compositions.
func genAccountFilter(seeds []fieldSeed) *commonpb.QueryFilter {
	switch {
	case oneIn(8):
		// A Field leaf on an UNDECLARED key: the index-not-found probe.
		return filterMetaExists("undeclared-" + metaKey())
	case oneIn(12):
		// A bare kind-mismatched Field leaf: the FILTER_COMPILATION probe.
		if f := genMismatchedFieldLeaf(seeds); f != nil {
			return f
		}

		return filterMetaExists(metaKey())
	case oneIn(6):
		return genAccountAssetFilter()
	case oneIn(16):
		// Target-invalid probe: a transactions-only condition, rejected on accounts.
		if random.RandomChoice([]uint8{0, 1}) == 0 {
			return filterReverted(true)
		}

		return filterTxIDRange(0, 255)
	case oneIn(4):
		return nil // top-level universe (no filter)
	case random.RandomChoice([]uint8{0, 1}) == 0:
		return genAccountFilterIndexed(seeds, 0)
	default:
		return genAccountFilterFree(0)
	}
}

// genAccountFilterIndexed rolls an accounts filter mixing metadata Field
// leaves (on declared keys) with the index-free address leaves. Field results
// select from the same current V+M universe address leaves scan — an account
// carrying metadata always has an attributes row — so booleans compose without
// the ever-touched-universe caveat that keeps has-asset bare.
func genAccountFilterIndexed(seeds []fieldSeed, depth int) *commonpb.QueryFilter {
	if depth >= maxQueryGenDepth || random.RandomChoice([]uint8{0, 1}) == 0 {
		if f := genFieldLeaf(seeds); f != nil && random.RandomChoice([]uint8{0, 1, 2}) != 0 {
			return f
		}

		return genAccountFilterFree(depth)
	}

	return genBoolean(depth, func(d int) *commonpb.QueryFilter { return genAccountFilterIndexed(seeds, d) })
}

// genAccountFilterFree rolls a non-nil index-free accounts filter: an address
// prefix/exact leaf, or a boolean composition of the same. It never returns nil
// — universe is expressible only as the absent top-level filter (a nil child in
// a repeated And/Or field marshals as an empty condition the compiler rejects).
func genAccountFilterFree(depth int) *commonpb.QueryFilter {
	if depth >= maxQueryGenDepth || random.RandomChoice([]uint8{0, 1}) == 0 {
		if random.RandomChoice([]uint8{0, 1}) == 0 {
			return filterAddrPrefix(poolName() + ":")
		}

		return filterAddrExact(poolAddress())
	}

	return genBoolean(depth, genAccountFilterFree)
}

// txFilterSeeds carries committed-state samples the transaction filter
// generator aims at, so index-backed leaves hit live data instead of always
// missing: committed references and learned date stamps. Sampled under the
// checker's lock (sampleTxFilterSeeds) — the generator itself runs outside it.
type txFilterSeeds struct {
	refs   []string
	stamps []uint64
	fields []fieldSeed
}

// sampleTxFilterSeeds snapshots up to a handful of committed references and
// known date stamps of the ledger. Acquires c.mu.
func (c *Checker) sampleTxFilterSeeds(ledger string) txFilterSeeds {
	c.mu.Lock()
	defer c.mu.Unlock()

	var seeds txFilterSeeds

	ls := c.modelState.Ledger(ledger)
	for ref := range ls.TxByRef().All() {
		seeds.refs = append(seeds.refs, ref)
		if len(seeds.refs) == 4 {
			break
		}
	}

	txs := ls.Txs()
	for range 4 {
		if txs.Len() == 0 {
			break
		}

		rec := txs.Get(internal.Rand().Intn(txs.Len()))
		for _, ts := range []*commonpb.Timestamp{rec.Timestamp(), rec.InsertedAt(), rec.RevertedAt()} {
			if ts != nil {
				seeds.stamps = append(seeds.stamps, ts.GetData())
			}
		}
	}

	seeds.fields = sampleFieldSeeds(ls, commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS)

	return seeds
}

// genTransactionFilter rolls a query filter for ListTransactions. One-in-six is
// an index-backed metadata condition (a never-built index — the NotFound path);
// ~1-in-16 is an accounts-only condition invalid on this target (the
// InvalidArgument path); then one-in-four is the no-filter universe; the rest
// split between the index-backed tx-builtin grammar (reference / date leaves,
// freely composed with index-free ones) and the pure index-free grammar.
func genTransactionFilter(seeds txFilterSeeds) *commonpb.QueryFilter {
	switch {
	case oneIn(8):
		// A Field leaf on an UNDECLARED key: the index-not-found probe.
		return filterMetaExists("undeclared-" + metaKey())
	case oneIn(12):
		// A bare kind-mismatched Field leaf: the FILTER_COMPILATION probe.
		if f := genMismatchedFieldLeaf(seeds.fields); f != nil {
			return f
		}

		return filterMetaExists(metaKey())
	case oneIn(16):
		// Target-invalid probe: an accounts-only condition, rejected on transactions.
		return filterHasAsset("USD", 2)
	case random.RandomChoice([]uint8{0, 1, 2, 3}) == 0:
		return nil // top-level universe (no filter)
	case random.RandomChoice([]uint8{0, 1}) == 0:
		return genTransactionFilterIndexed(seeds, 0)
	default:
		return genTransactionFilterFree(0)
	}
}

// genTransactionFilterIndexed rolls a transactions filter whose leaves include
// the index-backed tx builtins — reference and the three date fields — mixed
// with the index-free leaves. Unlike has-asset on accounts, every tx leaf
// selects from the same transaction-log universe, so composition needs no
// special casing: the window evaluator handles any boolean of these.
func genTransactionFilterIndexed(seeds txFilterSeeds, depth int) *commonpb.QueryFilter {
	if depth >= maxQueryGenDepth || random.RandomChoice([]uint8{0, 1}) == 0 {
		switch random.RandomChoice([]uint8{0, 1, 2, 3, 4, 5}) {
		case 0:
			return filterReference(seedReference(seeds))
		case 1, 2:
			return genDateLeaf(seeds)
		case 3:
			return genTxAddressLeaf()
		case 4:
			if f := genFieldLeaf(seeds.fields); f != nil {
				return f
			}

			return genTransactionFilterFree(depth)
		default:
			return genTransactionFilterFree(depth)
		}
	}

	return genBoolean(depth, func(d int) *commonpb.QueryFilter { return genTransactionFilterIndexed(seeds, d) })
}

// genTxAddressLeaf rolls an address leaf for the transactions target: a pool
// address (exact or cut to a prefix), or an unmatchable prefix, under a random
// role. Pool addresses re-target heavily, so exact and prefix variants both
// straddle live account→tx rows.
func genTxAddressLeaf() *commonpb.QueryFilter {
	roles := []commonpb.AddressRole{
		commonpb.AddressRole_ADDRESS_ROLE_ANY,
		commonpb.AddressRole_ADDRESS_ROLE_SOURCE,
		commonpb.AddressRole_ADDRESS_ROLE_DESTINATION,
	}
	role := roles[int(random.RandomChoice([]uint8{0, 1, 2}))]

	addr := poolAddress()
	switch random.RandomChoice([]uint8{0, 1, 2, 3}) {
	case 0:
		return filterAddrExactRole(addr, role)
	case 1:
		return filterAddrExactRole("world", role)
	case 2:
		// The "t-N:" one-type prefix; ":"-terminated so it cannot over-match.
		return filterAddrPrefixRole(addr[:strings.IndexByte(addr, ':')+1], role)
	default:
		if oneIn(8) {
			return filterAddrPrefixRole("absent:", role)
		}

		return filterAddrPrefixRole("t-", role)
	}
}

// seedReference picks a committed reference most of the time, a miss otherwise
// (an unknown reference, or the empty string — never indexed, matches nothing).
func seedReference(seeds txFilterSeeds) string {
	switch {
	case len(seeds.refs) > 0 && !oneIn(4):
		return seeds.refs[internal.Rand().Intn(len(seeds.refs))]
	case oneIn(8):
		return ""
	default:
		return "t-ref:absent"
	}
}

// genDateLeaf rolls a range condition on one of the three date builtins,
// bounded by committed stamps so it actually straddles live records: two seed
// stamps as [min, max] (single-sided or exclusive variants occasionally), or
// unconstrained bounds when the ledger has no known stamps yet.
func genDateLeaf(seeds txFilterSeeds) *commonpb.QueryFilter {
	fields := []commonpb.TransactionBuiltinIndex{
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT,
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT,
	}
	field := fields[int(random.RandomChoice([]uint8{0, 1, 2}))]

	pick := func() uint64 {
		if len(seeds.stamps) == 0 {
			return internal.Rand().Uint64() % 1024
		}

		return seeds.stamps[internal.Rand().Intn(len(seeds.stamps))]
	}

	a, b := pick(), pick()
	if a > b {
		a, b = b, a
	}

	f := filterDateRange(field, a, b)
	cond := f.GetBuiltinUint().GetCond()
	if oneIn(4) {
		cond.Min = nil
	} else {
		cond.MinExclusive = oneIn(4)
	}
	if oneIn(4) {
		cond.Max = nil
	} else {
		cond.MaxExclusive = oneIn(4)
	}

	return f
}

// genTransactionFilterFree rolls a non-nil index-free transactions filter: a
// reverted or tx-id-range leaf, or a boolean composition of the same. Like the
// accounts variant it never returns nil.
func genTransactionFilterFree(depth int) *commonpb.QueryFilter {
	if depth >= maxQueryGenDepth || random.RandomChoice([]uint8{0, 1}) == 0 {
		switch random.RandomChoice([]uint8{0, 1, 2}) {
		case 0:
			return filterReverted(true)
		case 1:
			return filterReverted(false)
		default:
			lo := internal.Rand().Uint64() % 256
			return filterTxIDRange(lo, lo+internal.Rand().Uint64()%256)
		}
	}

	return genBoolean(depth, genTransactionFilterFree)
}

// genBoolean wraps two (And/Or) or one (Not) recursively-generated children in a
// boolean combinator. gen never returns nil, so no combinator carries a nil
// child (which would marshal as an empty condition the compiler rejects).
func genBoolean(depth int, gen func(int) *commonpb.QueryFilter) *commonpb.QueryFilter {
	switch random.RandomChoice([]uint8{0, 1, 2}) {
	case 0:
		return filterAnd(gen(depth+1), gen(depth+1))
	case 1:
		return filterOr(gen(depth+1), gen(depth+1))
	default:
		return filterNot(gen(depth + 1))
	}
}

// --- Filter constructors ------------------------------------------------

func filterAddrPrefix(prefix string) *commonpb.QueryFilter {
	return filterAddrPrefixRole(prefix, commonpb.AddressRole_ADDRESS_ROLE_ANY)
}

func filterAddrPrefixRole(prefix string, role commonpb.AddressRole) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedPrefix{HardcodedPrefix: prefix},
		Role:  role,
	}}}
}

func filterAddrExact(addr string) *commonpb.QueryFilter {
	return filterAddrExactRole(addr, commonpb.AddressRole_ADDRESS_ROLE_ANY)
}

func filterAddrExactRole(addr string, role commonpb.AddressRole) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Address{Address: &commonpb.AddressMatch{
		Match: &commonpb.AddressMatch_HardcodedExact{HardcodedExact: addr},
		Role:  role,
	}}}
}

func filterReference(value string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reference{Reference: &commonpb.ReferenceCondition{
		Cond: &commonpb.StringCondition{Value: &commonpb.StringCondition_Hardcoded{Hardcoded: value}},
	}}}
}

func filterReverted(value bool) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Reverted{
		Reverted: &commonpb.RevertedCondition{Value: value},
	}}
}

// filterTxIDRange matches transactions with lo <= id <= hi. Both bounds are
// inclusive (no exclusive flags), mirroring resolveUintBounds' [min, max+1).
func filterTxIDRange(lo, hi uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
		Field: commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID,
		Cond:  &commonpb.UintCondition{Min: &lo, Max: &hi},
	}}}
}

// filterDateRange matches transactions whose `field` date lies in [lo, hi],
// both bounds inclusive (like filterTxIDRange).
func filterDateRange(field commonpb.TransactionBuiltinIndex, lo, hi uint64) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_BuiltinUint{BuiltinUint: &commonpb.BuiltinUintCondition{
		Field: field,
		Cond:  &commonpb.UintCondition{Min: &lo, Max: &hi},
	}}}
}

func filterAnd(children ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_And{And: &commonpb.AndFilter{Filters: children}}}
}

func filterOr(children ...*commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Or{Or: &commonpb.OrFilter{Filters: children}}}
}

func filterNot(child *commonpb.QueryFilter) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Not{Not: &commonpb.NotFilter{Filter: child}}}
}

// filterMetaExists matches entities that carry metadata key — an existence
// condition, index-backed on both targets. On a declared, indexed key it is a
// result-returning filter; on an undeclared key it is the index-not-found
// probe.
func filterMetaExists(key string) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{
		Field:     &commonpb.FieldRef{Metadata: key},
		Condition: &commonpb.FieldCondition_ExistsCond{ExistsCond: &commonpb.ExistsCondition{}},
	}}}
}

// filterHasAsset matches accounts holding (assetBase, precision). Valid only on
// the ACCOUNTS target, so on transactions it is the target-invalid probe
// (rejected with InvalidArgument before any index check). The values are
// immaterial — rejection happens on target validity, before evaluation.
func filterHasAsset(assetBase string, precision uint32) *commonpb.QueryFilter {
	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_AccountHasAsset{
		AccountHasAsset: &commonpb.AccountHasAssetCondition{AssetBase: assetBase, Precision: precision},
	}}
}

// --- Filter evaluation --------------------------------------------------

// filterNeedsIndex reports whether f contains any condition the server's
// compiler serves from a created index (query.Compile → requireIndexReady): the
// account-by-asset builtin, or any other index-backed leaf (metadata Field,
// Reference, timestamp builtins, address-on-transactions). The index-free
// conditions are universe (nil), address on accounts, reverted, and the tx-id
// builtin. See indexNeeds for the asset/other split the account-query lifecycle
// path keys on.
func filterNeedsIndex(f *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	asset, other := indexNeeds(f, target)

	return asset || other
}

// neededIndexCanonicals collects the canonical IndexIDs a filter needs the
// compiler to find READY on the given target: the tx builtins (reference, the
// date fields, the address roles), the account-by-asset builtin, and the
// per-(target, key) metadata indexes — plus a sentinel for any index-backed
// leaf whose index the workload never creates. The sentinel is never in the
// model's index set, so those filters always predict rejection. The set drives
// the per-index lifecycle validation (validateIndexedTransactionQuery /
// validateIndexedAccountQuery); empty means the filter is index-free and
// validates exactly. Target-invalid leaves classify like their home target —
// callers consult filterInvalidForTarget first, mirroring the compiler's
// check order.
func neededIndexCanonicals(f *commonpb.QueryFilter, target commonpb.QueryTarget, out map[string]struct{}) {
	if f == nil {
		return
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range x.And.GetFilters() {
			neededIndexCanonicals(child, target, out)
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range x.Or.GetFilters() {
			neededIndexCanonicals(child, target, out)
		}
	case *commonpb.QueryFilter_Not:
		neededIndexCanonicals(x.Not.GetFilter(), target, out)
	case *commonpb.QueryFilter_Field:
		out[metadataCanonical(target, x.Field.GetField().GetMetadata())] = struct{}{}
	case *commonpb.QueryFilter_AccountHasAsset:
		out[assetIndexCanonical] = struct{}{}
	case *commonpb.QueryFilter_Reference:
		out[txBuiltinCanonical(commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REFERENCE)] = struct{}{}
	case *commonpb.QueryFilter_Address:
		// Index-free on accounts (existence scan); the account→tx mapping
		// index on transactions.
		if target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS {
			out[txBuiltinCanonical(addressRoleBuiltin(x.Address.GetRole()))] = struct{}{}
		}
	case *commonpb.QueryFilter_BuiltinUint:
		if x.BuiltinUint.GetField() != commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID {
			out[txBuiltinCanonical(x.BuiltinUint.GetField())] = struct{}{}
		}
	case *commonpb.QueryFilter_Reverted:
		// index-free
	default:
		// Log conditions and future leaves: index-backed, never built here.
		out[neverBuiltIndexCanonical] = struct{}{}
	}
}

// neverBuiltIndexCanonical is the sentinel canonical for index-backed leaves
// the workload never builds an index for. It never appears in the model's index
// set, so IndexState reports it absent — a rejection is always legal, results
// never are.
const neverBuiltIndexCanonical = "workload:never-built"

// indexNeeds classifies f's index requirements into two buckets: `asset` is set
// if any leaf needs the account-by-asset builtin (an AccountHasAsset condition),
// `other` if any leaf needs a different index (metadata Field, Reference, a
// timestamp builtin, address-on-transactions). A filter is asset-only — its
// outcome governed by the account-by-asset lifecycle — iff asset && !other; the
// driver creates only that index, so any `other` need means a missing index and
// a rejected compile regardless. Combinators OR their children's needs; the
// compiler fails the whole query on the first missing index, so a single
// index-backed leaf anywhere sets the whole tree's need.
func indexNeeds(f *commonpb.QueryFilter, target commonpb.QueryTarget) (asset, other bool) {
	if f == nil {
		return false, false
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range x.And.GetFilters() {
			a, o := indexNeeds(child, target)
			asset, other = asset || a, other || o
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range x.Or.GetFilters() {
			a, o := indexNeeds(child, target)
			asset, other = asset || a, other || o
		}
	case *commonpb.QueryFilter_Not:
		return indexNeeds(x.Not.GetFilter(), target)
	case *commonpb.QueryFilter_AccountHasAsset:
		return true, false
	case *commonpb.QueryFilter_Reverted:
		return false, false
	case *commonpb.QueryFilter_Address:
		// Index-free on accounts; on transactions it needs the account→tx index.
		return false, target == commonpb.QueryTarget_QUERY_TARGET_TRANSACTIONS
	case *commonpb.QueryFilter_BuiltinUint:
		// Only the id builtin scans the always-present Pebble tx keyspace; the
		// timestamp/inserted_at/reverted_at builtins need an index.
		return false, x.BuiltinUint.GetField() != commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID
	default:
		// Field, Reference, log conditions — index-backed, non-asset.
		return false, true
	}

	return asset, other
}

// filterInvalidForTarget reports whether f carries any condition the server's
// per-target validity table rejects (rejectInvalidCondition → InvalidArgument) —
// e.g. reverted / tx-id on accounts, or account-has-asset on transactions. It
// reuses the exact commonpb functions the compiler consults, so the model's
// verdict cannot drift from the server's. Combinators are always valid; recurse
// into their children (mirroring the compiler's per-node check). The server
// checks validity before index availability, so callers must consult this
// before filterNeedsIndex.
func filterInvalidForTarget(f *commonpb.QueryFilter, target commonpb.QueryTarget) bool {
	if f == nil {
		return false
	}

	if !commonpb.ConditionValidForTarget(target, commonpb.ConditionKindOf(f)) {
		return true
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_And:
		for _, child := range x.And.GetFilters() {
			if filterInvalidForTarget(child, target) {
				return true
			}
		}
	case *commonpb.QueryFilter_Or:
		for _, child := range x.Or.GetFilters() {
			if filterInvalidForTarget(child, target) {
				return true
			}
		}
	case *commonpb.QueryFilter_Not:
		return filterInvalidForTarget(x.Not.GetFilter(), target)
	}

	return false
}

// matchAccountFilter evaluates an accounts filter against one address in ls.
// Empty And/Or match nothing, mirroring the compiler's empty-iterator treatment;
// a nil node is the universe (always matches). The AccountHasAsset arm needs the
// account's volumes, so ls is threaded through even though the index-free arms
// depend only on the address.
func matchAccountFilter(ls oracle.LedgerState, f *commonpb.QueryFilter, addr string) bool {
	if f == nil {
		return true
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Address:
		switch m := x.Address.GetMatch().(type) {
		case *commonpb.AddressMatch_HardcodedPrefix:
			return strings.HasPrefix(addr, m.HardcodedPrefix)
		case *commonpb.AddressMatch_HardcodedExact:
			return addr == m.HardcodedExact
		}

		return false
	case *commonpb.QueryFilter_AccountHasAsset:
		return accountHasAsset(ls, addr, x.AccountHasAsset.GetAssetBase(), x.AccountHasAsset.GetPrecision())
	case *commonpb.QueryFilter_Field:
		return matchFieldCondition(ls.AccountFieldTypes(), func(key string) (*commonpb.MetadataValue, bool) {
			v, ok := ls.Metadata().Get(oracle.MetaKey{Address: addr, Key: key})

			return v, ok
		}, x.Field)
	case *commonpb.QueryFilter_And:
		return matchAll(x.And.GetFilters(), func(child *commonpb.QueryFilter) bool { return matchAccountFilter(ls, child, addr) })
	case *commonpb.QueryFilter_Or:
		return matchAny(x.Or.GetFilters(), func(child *commonpb.QueryFilter) bool { return matchAccountFilter(ls, child, addr) })
	case *commonpb.QueryFilter_Not:
		return !matchAccountFilter(ls, x.Not.GetFilter(), addr)
	default:
		return false
	}
}

// matchTxFilter evaluates an index-free transactions filter against one record.
// matchTxFilter evaluates f on rec three-valued: known=false means the leaf
// verdict hinges on a server stamp the model has not learned yet (a date field
// of a bulk still in flight — see oracle.LearnTxStamps). Booleans propagate
// unknowns Kleene-style: a decided AND/OR short-circuits, an undecided one
// stays unknown. Window construction turns unknown rows into optional ones.
func matchTxFilter(ls oracle.LedgerState, f *commonpb.QueryFilter, rec txRecordView) (match, known bool) {
	if f == nil {
		return true, true
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Reverted:
		return rec.Reverted() == x.Reverted.GetValue(), true
	case *commonpb.QueryFilter_Reference:
		// Exact match on the null-terminated txref key; empty references are
		// never written to the index, so they can never match.
		return rec.Reference() != "" && rec.Reference() == x.Reference.GetCond().GetHardcoded(), true
	case *commonpb.QueryFilter_Address:
		return matchTxAddress(ls, x.Address, rec), true
	case *commonpb.QueryFilter_Field:
		return matchFieldCondition(ls.TransactionFieldTypes(), func(key string) (*commonpb.MetadataValue, bool) {
			v, ok := rec.Metadata()[key]

			return v, ok
		}, x.Field), true
	case *commonpb.QueryFilter_BuiltinUint:
		return matchTxBuiltinUint(x.BuiltinUint, rec)
	case *commonpb.QueryFilter_And:
		match, known = true, true
		for _, child := range x.And.GetFilters() {
			m, k := matchTxFilter(ls, child, rec)
			if k && !m {
				return false, true
			}
			known = known && k
		}

		return match, known
	case *commonpb.QueryFilter_Or:
		match, known = false, true
		for _, child := range x.Or.GetFilters() {
			m, k := matchTxFilter(ls, child, rec)
			if k && m {
				return true, true
			}
			known = known && k
		}

		return match, known
	case *commonpb.QueryFilter_Not:
		m, k := matchTxFilter(ls, x.Not.GetFilter(), rec)

		return !m, k
	default:
		return false, true
	}
}

// matchTxAddress evaluates an address leaf on the TRANSACTIONS target: the
// transaction matches iff some account in its account→tx index membership
// (IndexedAddrs, role-filtered) matches the prefix/exact pattern AND is still
// in the merged V+M account universe — the server resolves matching accounts
// through the attributes zone (pebbleAccountExists / the account prefix
// iterator), so a purged account with no metadata stops reaching its
// transactions even though the index rows remain.
func matchTxAddress(ls oracle.LedgerState, am *commonpb.AddressMatch, rec txRecordView) bool {
	var roleMask uint8
	switch am.GetRole() {
	case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
		roleMask = oracle.AddrIndexedSource
	case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
		roleMask = oracle.AddrIndexedDestination
	default:
		roleMask = oracle.AddrIndexedSource | oracle.AddrIndexedDestination
	}

	for addr, bits := range rec.IndexedAddrs() {
		if bits&roleMask == 0 {
			continue
		}

		switch m := am.GetMatch().(type) {
		case *commonpb.AddressMatch_HardcodedPrefix:
			if !strings.HasPrefix(addr, m.HardcodedPrefix) {
				continue
			}
		case *commonpb.AddressMatch_HardcodedExact:
			if addr != m.HardcodedExact {
				continue
			}
		default:
			continue // param matches are not generated
		}

		if ls.HasAccount(addr) {
			return true
		}
	}

	return false
}

// matchTxBuiltinUint evaluates a transaction builtin-uint leaf. The date
// builtins read the record's (possibly learned) server stamps; a still-unknown
// stamp is an unknown verdict, not a miss — the record may well be in the
// server's index.
func matchTxBuiltinUint(cond *commonpb.BuiltinUintCondition, rec txRecordView) (match, known bool) {
	switch cond.GetField() {
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:
		return matchTxIDBounds(cond.GetCond(), rec.Id()), true
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:
		if rec.Timestamp() == nil {
			return false, false
		}

		return matchTxIDBounds(cond.GetCond(), rec.Timestamp().GetData()), true
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT:
		if rec.InsertedAt() == nil {
			return false, false
		}

		return matchTxIDBounds(cond.GetCond(), rec.InsertedAt().GetData()), true
	case commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT:
		// Only reverted originals carry an rvat row. An un-reverted record is a
		// known miss; a reverted one with an unlearned revert stamp is unknown.
		if !rec.Reverted() {
			return false, true
		}
		if rec.RevertedAt() == nil {
			return false, false
		}

		return matchTxIDBounds(cond.GetCond(), rec.RevertedAt().GetData()), true
	default:
		return false, true
	}
}

// matchTxIDBounds mirrors resolveUintBounds: min/max honor their exclusive
// flags, and an absent bound is open on that side.
func matchTxIDBounds(cond *commonpb.UintCondition, id uint64) bool {
	if cond.Min != nil {
		if cond.GetMinExclusive() {
			if id <= cond.GetMin() {
				return false
			}
		} else if id < cond.GetMin() {
			return false
		}
	}

	if cond.Max != nil {
		if cond.GetMaxExclusive() {
			if id >= cond.GetMax() {
				return false
			}
		} else if id > cond.GetMax() {
			return false
		}
	}

	return true
}

// matchAll reports whether every child matches; an empty set matches nothing,
// mirroring the compiler's empty-And iterator.
func matchAll(children []*commonpb.QueryFilter, match func(*commonpb.QueryFilter) bool) bool {
	if len(children) == 0 {
		return false
	}

	for _, child := range children {
		if !match(child) {
			return false
		}
	}

	return true
}

// matchAny reports whether some child matches; an empty set matches nothing.
func matchAny(children []*commonpb.QueryFilter, match func(*commonpb.QueryFilter) bool) bool {
	for _, child := range children {
		if match(child) {
			return true
		}
	}

	return false
}

// --- helpers ------------------------------------------------------------

func reverseStrings(s []string) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
}

func joinUint64(s []uint64) string {
	parts := make([]string, len(s))
	for i, v := range s {
		parts[i] = strconv.FormatUint(v, 10)
	}

	return strings.Join(parts, ",")
}

// describeFilter renders a filter as a compact prefix expression for assertion
// details and debug logs.
func describeFilter(f *commonpb.QueryFilter) string {
	if f == nil {
		return "*"
	}

	switch x := f.GetFilter().(type) {
	case *commonpb.QueryFilter_Address:
		role := ""
		switch x.Address.GetRole() {
		case commonpb.AddressRole_ADDRESS_ROLE_SOURCE:
			role = "/src"
		case commonpb.AddressRole_ADDRESS_ROLE_DESTINATION:
			role = "/dst"
		}

		switch m := x.Address.GetMatch().(type) {
		case *commonpb.AddressMatch_HardcodedPrefix:
			return "addr^" + m.HardcodedPrefix + role
		case *commonpb.AddressMatch_HardcodedExact:
			return "addr=" + m.HardcodedExact + role
		}

		return "addr?"
	case *commonpb.QueryFilter_Reverted:
		return "reverted=" + strconv.FormatBool(x.Reverted.GetValue())
	case *commonpb.QueryFilter_Reference:
		return "ref=" + x.Reference.GetCond().GetHardcoded()
	case *commonpb.QueryFilter_BuiltinUint:
		return describeBuiltinUint(x.BuiltinUint)
	case *commonpb.QueryFilter_And:
		return "and(" + describeChildren(x.And.GetFilters()) + ")"
	case *commonpb.QueryFilter_Or:
		return "or(" + describeChildren(x.Or.GetFilters()) + ")"
	case *commonpb.QueryFilter_Not:
		return "not(" + describeFilter(x.Not.GetFilter()) + ")"
	case *commonpb.QueryFilter_Field:
		return "field:" + x.Field.GetField().GetMetadata()
	case *commonpb.QueryFilter_AccountHasAsset:
		return "hasAsset:" + x.AccountHasAsset.GetAssetBase()
	default:
		return "?"
	}
}

// describeBuiltinUint renders a builtin-uint leaf: the field's short name and
// its bounds, exclusive bounds parenthesized, absent ones as "_".
func describeBuiltinUint(c *commonpb.BuiltinUintCondition) string {
	name := map[commonpb.TransactionBuiltinIndex]string{
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_ID:          "id",
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_TIMESTAMP:   "ts",
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_INSERTED_AT: "iat",
		commonpb.TransactionBuiltinIndex_TX_BUILTIN_INDEX_REVERTED_AT: "rvat",
	}[c.GetField()]
	if name == "" {
		name = c.GetField().String()
	}

	cond := c.GetCond()
	lo, hi := "_", "_"
	if cond.Min != nil {
		lo = strconv.FormatUint(cond.GetMin(), 10)
		if cond.GetMinExclusive() {
			lo = "(" + lo
		}
	}
	if cond.Max != nil {
		hi = strconv.FormatUint(cond.GetMax(), 10)
		if cond.GetMaxExclusive() {
			hi += ")"
		}
	}

	return name + "[" + lo + "," + hi + "]"
}

func describeChildren(children []*commonpb.QueryFilter) string {
	parts := make([]string, 0, len(children))
	for _, child := range children {
		parts = append(parts, describeFilter(child))
	}

	return strings.Join(parts, ",")
}
