package main

import (
	"math"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle"
)

// committedStateWithSequences is a one-ledger committed state holding one
// account-metadata log per sequence given, each learned at that sequence.
func committedStateWithSequences(t *testing.T, sequences ...uint64) oracle.GlobalState {
	t.Helper()

	reqs := make([]*servicepb.Request, 0, len(sequences))
	for i := range sequences {
		reqs = append(reqs, saveAccountMetaReqL("L", "acc:1", "k"+strconv.Itoa(i), "v"))
	}

	res := oracle.NewGlobalState().Apply(oracle.Bulk{Requests: reqs})
	require.True(t, res.OK, "setup bulk rejected: %s", res.Reason)

	for i, seq := range sequences {
		res.State.LearnLogSequence("L", uint64(i+1), seq)
	}

	return res.State
}

// metadataLogs is the committed-log record committedStateWithSequences builds:
// one account-metadata save per sequence, in order, on ledger L.
func metadataLogs(sequences ...uint64) map[uint64]committedLog {
	out := make(map[uint64]committedLog, len(sequences))
	for i, seq := range sequences {
		out[seq] = committedLog{ledger: "L", id: uint64(i + 1), kind: "saved_metadata"}
	}

	return out
}

// singleOrderBulks is the boundary record for a run of one-order bulks, each
// committing at one sequence — the shape committedStateWithSequences builds.
func singleOrderBulks(sequences ...uint64) map[uint64]committedBulk {
	out := make(map[uint64]committedBulk, len(sequences))
	for _, seq := range sequences {
		out[seq] = committedBulk{minSeq: seq, maxSeq: seq, orders: 1}
	}

	return out
}

func TestAuditSuccessMismatchPinsOneBulk(t *testing.T) {
	t.Parallel()

	logs := map[uint64]committedLog{
		10: {ledger: "a", id: 1}, 11: {ledger: "b", id: 1}, 12: {ledger: "a", id: 2},
	}
	entry := auditEntry{seq: 7, ledgers: []string{"a", "b"}, orderCount: 3, minLog: 10, maxLog: 12, itemSeqs: []uint64{10, 11, 12}}
	bulks := map[uint64]committedBulk{10: {minSeq: 10, maxSeq: 12, orders: 3}}

	why, _ := auditSuccessMismatch(entry, logs, bulks, 12)
	require.Empty(t, why)

	archived := entry
	archived.itemSeqs = nil
	why, _ = auditSuccessMismatch(archived, logs, bulks, 12)
	require.Empty(t, why, "purged items are not a mismatch")

	e := entry
	e.ledgers = []string{"a"}
	why, _ = auditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "entry names a")

	e = entry
	e.orderCount = 2
	why, _ = auditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "order count")

	e = entry
	e.itemSeqs = []uint64{10, 10, 12}
	why, _ = auditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "repeated", "a repeated item leaves log 11 without one")

	e = entry
	e.maxLog = 13
	why, _ = auditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "straddles")

	e = entry
	e.itemSeqs = []uint64{10, 11, 99}
	why, _ = auditSuccessMismatch(e, logs, bulks, 12)
	require.Contains(t, why, "outside the entry")

	// One entry per bulk: a contiguous run of committed logs is not enough when
	// the bulks behind it were separate.
	split := map[uint64]committedBulk{10: {minSeq: 10, maxSeq: 11, orders: 2}, 12: {minSeq: 12, maxSeq: 12, orders: 1}}
	why, _ = auditSuccessMismatch(entry, logs, split, 12)
	require.Contains(t, why, "ends at 11")

	merged := auditEntry{seq: 7, ledgers: []string{"b"}, orderCount: 1, minLog: 11, maxLog: 11}
	why, missing := auditSuccessMismatch(merged, logs, bulks, 12)
	require.Contains(t, why, "no committed bulk begins at 11")
	require.EqualValues(t, 11, missing)

	delete(logs, 11)
	why, _ = auditSuccessMismatch(entry, logs, bulks, 12)
	require.Contains(t, why, "no committed log")
}

func TestRejectionExplainsMatchesLedgersOrdersAndReason(t *testing.T) {
	t.Parallel()

	c := &Checker{rejections: map[rejectedBulk]struct{}{{ledgers: "a,b", orders: 2, reason: "INSUFFICIENT_FUNDS"}: {}}}

	require.True(t, c.rejectionExplains(auditEntry{failed: true, ledgers: []string{"a", "b"}, orderCount: 2, reason: "INSUFFICIENT_FUNDS"}))
	require.True(t, c.rejectionExplains(auditEntry{failed: true, ledgers: []string{"a", "b"}, orderCount: 2, reason: "UNSPECIFIED"}), "a reason the wire does not name still matches on ledgers and orders")
	require.False(t, c.rejectionExplains(auditEntry{failed: true, ledgers: []string{"a"}, orderCount: 2, reason: "INSUFFICIENT_FUNDS"}))
	require.False(t, c.rejectionExplains(auditEntry{failed: true, ledgers: []string{"a", "b"}, orderCount: 1, reason: "INSUFFICIENT_FUNDS"}))
	require.False(t, c.rejectionExplains(auditEntry{failed: true, ledgers: []string{"a", "b"}, orderCount: 2, reason: "VALIDATION"}))
}

func TestAuditPageViolationIsAnExtraOverTheModel(t *testing.T) {
	t.Parallel()

	ledgerA := filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "a")
	page := []auditEntry{{seq: 3, ledgers: []string{"a"}}, {seq: 2, ledgers: []string{"a"}}}
	require.Empty(t, auditPageViolation(page, 2, true, ledgerA, 0, ""))
	require.Equal(t, "page longer than requested", auditPageViolation(page, 1, true, ledgerA, 0, ""))
	require.Equal(t, "ascending order violated", auditPageViolation(page, 2, false, ledgerA, 0, ""))
	require.Equal(t, "entry outside the filter", auditPageViolation(page, 2, true, filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "b"), 0, ""))
	require.Equal(t, "entry outside the filter", auditPageViolation(page, 2, true, filterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"), 0, ""))
}

// A zone scan — no filter, a sequence bound, or an And of those — serves dense
// sequences from its lower bound; an indexed page may skip.
func TestAuditPageViolationZoneScanIsDense(t *testing.T) {
	t.Parallel()

	page := func(seqs ...uint64) []auditEntry {
		out := make([]auditEntry, 0, len(seqs))
		for _, s := range seqs {
			out = append(out, auditEntry{seq: s, ledgers: []string{"a"}})
		}

		return out
	}

	require.Empty(t, auditPageViolation(page(1, 2, 3), 10, false, nil, 0, ""))
	require.Equal(t, "gap in a zone-scan page", auditPageViolation(page(1, 3), 10, false, nil, 0, ""))
	require.Equal(t, "zone-scan page does not start at its lower bound", auditPageViolation(page(2, 3), 10, false, nil, 0, ""))

	lo := uint64(2)
	from2 := filterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo})
	require.Empty(t, auditPageViolation(page(2, 3), 10, false, from2, 0, ""))
	require.Empty(t, auditPageViolation(page(3, 4), 10, false, filterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo, MinExclusive: true}), 0, ""), "an exclusive bound starts one past it")
	require.Equal(t, "zone-scan page does not start at its lower bound", auditPageViolation(page(1, 2), 10, false, from2, 0, ""))
	require.Empty(t, auditPageViolation(page(5, 4, 3), 10, true, nil, 0, ""), "a reverse scan starts anywhere but stays dense")
	require.Equal(t, "gap in a zone-scan page", auditPageViolation(page(5, 3), 10, true, nil, 0, ""))

	indexed := filterAnd(from2, filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "a"))
	require.Empty(t, auditPageViolation(page(2, 5, 9), 10, false, indexed, 0, ""), "an indexed page selects sparsely")
}

// The trail is permanent, so an entry it served once must still be reachable.
// That makes the remembered set a lower bound on what any later page owes:
// rows while the page has room, a resume cursor once it is full.
func TestValidateAuditPageKnownEntriesAreStillOwed(t *testing.T) {
	t.Parallel()

	c := &Checker{
		ledgerNames: []string{"L"}, modelState: committedStateWithSequences(t, 42, 43, 44),
		inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]ledgerLogRecord{},
		rejections: map[rejectedBulk]struct{}{}, knownAudit: map[uint64]knownAuditEntry{},
		committedBulks: singleOrderBulks(42, 43, 44), committedLogs: metadataLogs(42, 43, 44),
	}
	e42 := auditEntry{seq: 3, ledgers: []string{"L"}, orderCount: 1, minLog: 42, maxLog: 42}
	e43 := auditEntry{seq: 4, ledgers: []string{"L"}, orderCount: 1, minLog: 43, maxLog: 43}
	e44 := auditEntry{seq: 5, ledgers: []string{"L"}, orderCount: 1, minLog: 44, maxLog: 44}

	// Nothing remembered yet: every shape of page is admissible.
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, nil, false, nil, 5, 0, "").finding)

	registered := c.auditLearnSeq
	c.noteKnownAuditEntries([]auditEntry{e42, e43, e44})

	// A read that registered before the entries were learned may have been
	// served from a snapshot older than them.
	require.Empty(t, c.validateAuditPage(0, registered, nil, false, nil, 5, 0, "").finding)

	require.Equal(t, "audit page stopped short of an entry the trail served before",
		c.validateAuditPage(0, c.auditLearnSeq, nil, false, nil, 5, 0, "").finding, "an empty page cannot be the answer")
	require.Equal(t, "audit page omits an entry the trail served before",
		c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e42, e44}, false, nil, 5, 0, "").finding, "4 sits inside the page's own range")
	require.Equal(t, "audit page stopped short of an entry the trail served before",
		c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e42, e43}, false, nil, 5, 0, "").finding, "the page had two rows left")

	// A full page owes a cursor naming where to resume, not the rows themselves.
	require.Equal(t, "audit page dropped its resume cursor",
		c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e42, e43}, false, nil, 2, 0, "").finding)
	full := c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e42, e43}, false, nil, 2, 0, "4")
	require.Empty(t, full.finding)
	require.True(t, full.cursorCorroborated, "entry 5 is what proved the cursor was owed")
	require.Equal(t, 3, full.knownCertified)

	// Entries at or before the cursor are another page's business.
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e44}, false, nil, 5, 4, "").finding)
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e43, e42}, true, nil, 5, 5, "").finding)
	require.Equal(t, "audit page stopped short of an entry the trail served before",
		c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e44, e43}, true, nil, 5, 0, "").finding, "3 is still below a descending page")
}

// A remembered entry only counts toward the guarantee when the filter certainly
// selects it. An order-type leaf is judged from the kinds the model committed
// under the entry's logs, so an entry it cannot account for proves nothing.
func TestValidateAuditPageKnownEntriesMustCertainlyMatch(t *testing.T) {
	t.Parallel()

	c := &Checker{
		ledgerNames: []string{"L"}, modelState: committedStateWithSequences(t, 42),
		inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]ledgerLogRecord{},
		rejections: map[rejectedBulk]struct{}{}, knownAudit: map[uint64]knownAuditEntry{},
		committedBulks: singleOrderBulks(42), committedLogs: metadataLogs(42),
	}

	// Log 42 is an account-metadata save, so its entry is an add_metadata order.
	known := auditEntry{seq: 3, ledgers: []string{"L"}, orderCount: 1, minLog: 42, maxLog: 42}
	c.noteKnownAuditEntries([]auditEntry{known})

	require.Equal(t, "audit page stopped short of an entry the trail served before",
		c.validateAuditPage(0, c.auditLearnSeq, nil, false, filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "add_metadata"), 5, 0, "").finding)
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, nil, false, filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction"), 5, 0, "").finding)
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, nil, false, filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "other"), 5, 0, "").finding)

	// A system-scoped entry names no logs, so no order type can be derived.
	c.noteKnownAuditEntries([]auditEntry{{seq: 9, orderCount: 1}})
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, nil, false, filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction"), 5, 0, "").finding)
}

// A resumed page starts past its cursor, in the iteration's own direction, and
// the token it hands back names the last entry it served.
func TestAuditPageViolationHonoursTheCursor(t *testing.T) {
	t.Parallel()

	page := func(seqs ...uint64) []auditEntry {
		out := make([]auditEntry, 0, len(seqs))
		for _, s := range seqs {
			out = append(out, auditEntry{seq: s, ledgers: []string{"a"}})
		}

		return out
	}

	require.Empty(t, auditPageViolation(page(4, 5), 10, false, nil, 3, ""))
	require.Equal(t, "entry outside the cursor", auditPageViolation(page(3, 4), 10, false, nil, 3, ""), "resume is exclusive")
	require.Empty(t, auditPageViolation(page(2, 1), 10, true, nil, 3, ""))
	require.Equal(t, "entry outside the cursor", auditPageViolation(page(3, 2), 10, true, nil, 3, ""))

	// The zone is dense past the cursor too, so a resumed scan starts on the
	// very next sequence.
	require.Equal(t, "zone-scan page does not start at its lower bound", auditPageViolation(page(5, 6), 10, false, nil, 3, ""))

	lo := uint64(8)
	from8 := filterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo})
	require.Empty(t, auditPageViolation(page(8, 9), 10, false, from8, 3, ""), "the filter's bound outranks a lower cursor")

	// The trail's universe is wider than the model's, so whether a token was
	// owed is never judged — but its value is, and only a full page can carry
	// one at all.
	require.Empty(t, auditPageViolation(page(4, 5), 2, false, nil, 3, "5"))
	require.Empty(t, auditPageViolation(page(4, 5), 2, false, nil, 3, ""), "a full page may equally have been the last one")
	require.Equal(t, "resume token does not match the page it rode with", auditPageViolation(page(4, 5), 2, false, nil, 3, "6"))
	require.Equal(t, "resume token does not match the page it rode with", auditPageViolation(page(4, 5), 10, false, nil, 3, "5"), "the page size did not cut this page, so the peek never fired")
	require.Equal(t, "resume token does not match the page it rode with", auditPageViolation(nil, 10, false, nil, 3, "5"))
}

// Every audit leaf is judged on the served entry's own fields.
func TestAuditEntrySatisfiesEveryLeaf(t *testing.T) {
	t.Parallel()

	ok := auditEntry{seq: 10, proposalID: 77, timestamp: 5_000_000, subject: "alice", ledgers: []string{"a", "b"}, minLog: 40, maxLog: 42}
	failed := auditEntry{seq: 11, ledgers: []string{"a"}, failed: true, reason: "VALIDATION"}
	u := func(lo, hi uint64) *commonpb.UintCondition { return &commonpb.UintCondition{Min: &lo, Max: &hi} }
	leaf := filterAuditUint
	str := filterAuditString

	require.True(t, auditEntrySatisfies(nil, ok))
	require.True(t, auditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, u(10, 10)), ok))
	require.False(t, auditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, u(11, 20)), ok))
	require.True(t, auditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_PROPOSAL_ID, u(70, 80)), ok))
	require.True(t, auditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_TIMESTAMP, u(4_000_000, 6_000_000)), ok))
	require.True(t, auditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, u(42, 50)), ok), "match-any over the entry's logs")
	require.False(t, auditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, u(43, 50)), ok))
	require.False(t, auditEntrySatisfies(leaf(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, u(0, 100)), failed), "a rejection has no logs")
	require.True(t, auditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "success"), ok))
	require.True(t, auditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure"), failed))
	require.True(t, auditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_LEDGER, "b"), ok))
	require.True(t, auditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, "alice"), ok))
	require.False(t, auditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_CALLER_SUBJECT, ""), failed), "an empty subject is never indexed")
	require.True(t, auditEntrySatisfies(str(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "anything"), ok), "order types are judged by the model")
	require.True(t, auditEntrySatisfies(filterAnd(str(commonpb.AuditField_AUDIT_FIELD_LEDGER, "a"), str(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "success")), ok))
	require.False(t, auditEntrySatisfies(filterOr(str(commonpb.AuditField_AUDIT_FIELD_LEDGER, "c"), str(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")), ok))

	// The exclusive extrema keep an empty range empty.
	zero := uint64(0)
	top := uint64(math.MaxUint64)
	require.False(t, boundsMeetRange(&commonpb.UintCondition{Max: &zero, MaxExclusive: true}, 0, 100))
	require.False(t, boundsMeetRange(&commonpb.UintCondition{Min: &top, MinExclusive: true}, 0, math.MaxUint64))
	require.True(t, boundsMeetRange(&commonpb.UintCondition{Max: &zero}, 0, 100))
}

// The kind-to-token table is the indexer's own derivation.
func TestAuditOrderTypeTableMatchesTheDomain(t *testing.T) {
	t.Parallel()

	ledgerScoped := func(o *raftcmdpb.LedgerScopedOrder) *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: o}}
	}
	apply := func(o *raftcmdpb.LedgerApplyOrder) *raftcmdpb.Order {
		return ledgerScoped(&raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: o}})
	}
	orders := map[string]*raftcmdpb.Order{
		"created_transaction":              apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateTransaction{}}),
		"reverted_transaction":             apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RevertTransaction{}}),
		"saved_metadata":                   apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddMetadata{}}),
		"deleted_metadata":                 apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DeleteMetadata{}}),
		"set_metadata_field_type":          apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_SetMetadataFieldType{}}),
		"removed_metadata_field_type":      apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveMetadataFieldType{}}),
		"create_index":                     apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_CreateIndex{}}),
		"drop_index":                       apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_DropIndex{}}),
		"added_account_type":               apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_AddAccountType{}}),
		"removed_account_type":             apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_RemoveAccountType{}}),
		"updated_default_enforcement_mode": apply(&raftcmdpb.LedgerApplyOrder{Data: &raftcmdpb.LedgerApplyOrder_UpdateDefaultEnforcementMode{}}),
		"saved_ledger_metadata":            ledgerScoped(&raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{}}),
		"deleted_ledger_metadata":          ledgerScoped(&raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{}}),
	}
	require.Len(t, orders, len(auditOrderTypeOfKind))
	for kind, order := range orders {
		require.Equal(t, domain.AuditOrderType(order), auditOrderTypeOfKind[kind], kind)
	}

	for _, token := range auditOrderTypeOfKind {
		require.Contains(t, auditOrderTypes, token)
	}
}

// An order-type page is judged against the kinds the model committed under
// the entry's logs, and a bare log-sequence page must cover every committed
// log in its range.
func TestValidateAuditPageOrderTypesAndLogCoverage(t *testing.T) {
	t.Parallel()

	// Two account-metadata logs at 42 and 44 (kind saved_metadata → add_metadata).
	c := &Checker{ledgerNames: []string{"L"}, modelState: committedStateWithSequences(t, 42, 44), inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]ledgerLogRecord{}, rejections: map[rejectedBulk]struct{}{}, committedBulks: singleOrderBulks(42, 43, 44), committedLogs: metadataLogs(42, 43, 44)}
	e42 := auditEntry{seq: 3, ledgers: []string{"L"}, orderCount: 1, minLog: 42, maxLog: 42}
	e44 := auditEntry{seq: 5, ledgers: []string{"L"}, orderCount: 1, minLog: 44, maxLog: 44}

	addMeta := filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "add_metadata")
	createTx := filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction")
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e42}, false, addMeta, 50, 0, "").finding)
	require.Equal(t, "audit entry outside the order-type scope", c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e42}, false, createTx, 50, 0, "").finding)

	c.ledgerLogSeqs[43] = ledgerLogRecord{ledger: "L", kind: "saved_ledger_metadata"}
	e43 := auditEntry{seq: 4, ledgers: []string{"L"}, orderCount: 1, minLog: 43, maxLog: 43}
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e43}, false, filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "save_ledger_metadata"), 50, 0, "").finding)
	require.Equal(t, "audit entry outside the order-type scope", c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e43}, false, filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "delete_ledger_metadata"), 50, 0, "").finding, "the order behind a ledger-metadata log is known, so the other token does not hold")

	lo, hi := uint64(40), uint64(50)
	logRange := filterAuditUint(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, &commonpb.UintCondition{Min: &lo, Max: &hi})
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e42, e43, e44}, false, logRange, 50, 0, "").finding)
	require.Equal(t, "audit trail misses a committed log", c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e42, e44}, false, logRange, 50, 0, "").finding, "43 is committed and in range")
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{e42, e44}, false, logRange, 2, 0, "").finding, "a full page may have been cut before 43")
}

// A reverse first page must reach the newest committed bulk unless the newest
// ledger-scoped entry is a rejection or a bulk is still in flight.
func TestValidateAuditPageReverseTail(t *testing.T) {
	t.Parallel()

	// Logs at 42 and 44; 43 is a hole inside the learned range.
	c := &Checker{ledgerNames: []string{"L"}, modelState: committedStateWithSequences(t, 42, 44), inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]ledgerLogRecord{}, rejections: map[rejectedBulk]struct{}{{ledgers: "L", orders: 1, reason: "VALIDATION"}: {}}, committedBulks: singleOrderBulks(42, 43, 44), committedLogs: metadataLogs(42, 44)}

	newest := auditEntry{seq: 5, ledgers: []string{"L"}, orderCount: 1, minLog: 44, maxLog: 44}
	older := auditEntry{seq: 3, ledgers: []string{"L"}, orderCount: 1, minLog: 42, maxLog: 42}
	hole := auditEntry{seq: 4, ledgers: []string{"L"}, orderCount: 1, minLog: 43, maxLog: 43}
	rejected := auditEntry{seq: 6, ledgers: []string{"L"}, orderCount: 1, failed: true, reason: "VALIDATION"}
	system := auditEntry{seq: 7}

	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{newest, older}, true, nil, 50, 0, "").finding)
	require.Equal(t, "audit entry outside model", c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{hole}, true, nil, 50, 0, "").finding, "43 is no committed log")

	res := c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{rejected, newest}, true, nil, 50, 0, "")
	require.Empty(t, res.finding)
	require.Equal(t, 1, res.rejections)

	require.Equal(t, "audit tail misses the newest committed bulk", c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{system, older}, true, nil, 50, 0, "").finding)
	systemAtFrontier := auditEntry{seq: 7, orderCount: 1, minLog: 44, maxLog: 44}
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{systemAtFrontier, older}, true, nil, 50, 0, "").finding,
		"a system order's log is committed too, and its entry covers the tail")
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{rejected, older}, true, nil, 50, 0, "").finding, "a newest rejection is the real tail")
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{older}, false, nil, 50, 0, "").finding, "only a reverse first page starts at the newest entry")

	// A filtered page selects its own newest entry, so the global frontier says
	// nothing about it.
	upTo3 := filterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Max: &[]uint64{3}[0]})
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{older}, true, upTo3, 50, 0, "").finding, "the newest bulk is outside this filter")

	c.inflight[1] = oracle.Bulk{}
	require.Empty(t, c.validateAuditPage(1, c.auditLearnSeq, []auditEntry{older}, true, nil, 50, 0, "").finding, "an undrained bulk may be the real tail")
}

// Entries the driver never drained — setup's ledgers and initial schema — are
// not judged, and a ledger-level log's sequence is learned outside the oracle.
func TestValidateAuditPageSetupEraAndLedgerLevelLogs(t *testing.T) {
	t.Parallel()

	c := &Checker{ledgerNames: []string{"L"}, modelState: committedStateWithSequences(t, 42), inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]ledgerLogRecord{}, committedBulks: singleOrderBulks(42, 43), committedLogs: metadataLogs(42), setupMaxSeq: 2}

	setup := auditEntry{seq: 1, ledgers: []string{"L"}, orderCount: 1, minLog: 2, maxLog: 2}
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{setup}, false, nil, 50, 0, "").finding, "at or below setup's last log is setup")

	ledgerLevel := auditEntry{seq: 8, ledgers: []string{"L"}, orderCount: 1, minLog: 43, maxLog: 43}
	require.Equal(t, "audit entry names logs the model never committed", c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{ledgerLevel}, false, nil, 50, 0, "").finding)

	c.ledgerLogSeqs[43] = ledgerLogRecord{ledger: "L", kind: "saved_ledger_metadata"}
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{ledgerLevel}, false, nil, 50, 0, "").finding, "a learned ledger-level sequence is a committed log")
}

// Before the first bulk drains the model has learned no sequence, so setup's
// entries are told apart by setup's own last log, not by what was learned.
func TestValidateAuditPageBeforeFirstDrain(t *testing.T) {
	t.Parallel()

	c := &Checker{ledgerNames: []string{"L"}, modelState: oracle.NewGlobalState(), inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]ledgerLogRecord{}, committedBulks: map[uint64]committedBulk{}, committedLogs: map[uint64]committedLog{}, setupMaxSeq: 8}

	createLedger := auditEntry{seq: 6, ledgers: []string{"L"}, orderCount: 1, minLog: 6, maxLog: 6}
	require.Empty(t, c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{createLedger}, true, nil, 50, 0, "").finding, "setup's CreateLedger is not judged")
	require.Empty(t, c.validateAuditEntry(0, createLedger).finding, "setup's CreateLedger is not judged")

	driver := auditEntry{seq: 9, ledgers: []string{"L"}, orderCount: 1, minLog: 9, maxLog: 9}
	require.Equal(t, "audit entry names logs the model never committed", c.validateAuditPage(0, c.auditLearnSeq, []auditEntry{driver, createLedger}, true, nil, 50, 0, "").finding)
	require.Equal(t, "audit entry names logs the model never committed", c.validateAuditEntry(0, driver).finding)

	c.inflight[1] = oracle.Bulk{}
	require.Empty(t, c.validateAuditPage(1, c.auditLearnSeq, []auditEntry{driver, createLedger}, true, nil, 50, 0, "").finding, "an undrained bulk may have committed it")
}

// The audit trail serves its whole history from the first page on, so a
// rejection or ledger-level log stays explainable however many newer ones the
// run records.
func TestAuditHistoryIsNeverPruned(t *testing.T) {
	t.Parallel()

	c := &Checker{rejections: map[rejectedBulk]struct{}{}, ledgerLogSeqs: map[uint64]ledgerLogRecord{}}
	first := oracle.Bulk{Requests: []*servicepb.Request{saveLedgerMetaReqL("L")}}
	c.recordRejection(first, "VALIDATION")
	c.learnLedgerLogSequences(first, []*commonpb.Log{{Sequence: 1, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_SavedLedgerMetadata{}}}})

	for i := uint64(2); i <= 20_000; i++ {
		later := oracle.Bulk{Requests: []*servicepb.Request{saveLedgerMetaReqL("L"), saveLedgerMetaReqL("L")}}
		c.recordRejection(later, "INSUFFICIENT_FUNDS")
		c.learnLedgerLogSequences(first, []*commonpb.Log{{Sequence: i, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_SavedLedgerMetadata{}}}})
	}

	require.True(t, c.rejectionExplains(auditEntry{failed: true, ledgers: []string{"L"}, orderCount: 1, reason: "VALIDATION"}))
	require.True(t, c.rejectionExplains(auditEntry{failed: true, ledgers: []string{"L"}, orderCount: 1, reason: "UNSPECIFIED"}), "an unnamed reason matches on ledgers and order count")
	require.False(t, c.rejectionExplains(auditEntry{failed: true, ledgers: []string{"L"}, orderCount: 1, reason: "INSUFFICIENT_FUNDS"}))
	require.Equal(t, ledgerLogRecord{ledger: "L", kind: "saved_ledger_metadata"}, c.ledgerLogSeqs[1])
}

func saveLedgerMetaReqL(ledger string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{
		SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{Ledger: ledger, Metadata: map[string]*commonpb.MetadataValue{"k": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}}}},
	}}
}

// requestLogKind names the accepted order, so every kind the token table maps
// has a request shape that produces it — otherwise an order-type filter judges
// the entry against an empty set and rejects a page the server served right.
func TestRequestLogKindCoversEveryAuditToken(t *testing.T) {
	t.Parallel()

	apply := func(action *servicepb.LedgerAction) *servicepb.Request {
		return &servicepb.Request{Type: &servicepb.Request_Apply{
			Apply: &servicepb.LedgerApplyRequest{Action: action},
		}}
	}

	reqs := []*servicepb.Request{
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_CreateTransaction{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_RevertTransaction{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddMetadata{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_DeleteMetadata{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_AddAccountType{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_RemoveAccountType{}}),
		apply(&servicepb.LedgerAction{Data: &servicepb.LedgerAction_SetDefaultEnforcementMode{}}),
		{Type: &servicepb.Request_SetMetadataFieldType{}},
		{Type: &servicepb.Request_RemoveMetadataFieldType{}},
		{Type: &servicepb.Request_CreateIndex{}},
		{Type: &servicepb.Request_DropIndex{}},
		{Type: &servicepb.Request_AddAccountType{}},
		{Type: &servicepb.Request_RemoveAccountType{}},
		{Type: &servicepb.Request_SetDefaultEnforcementMode{}},
		{Type: &servicepb.Request_SaveLedgerMetadata{}},
		{Type: &servicepb.Request_DeleteLedgerMetadata{}},
	}

	produced := map[string]bool{}
	for _, req := range reqs {
		kind := requestLogKind(req)
		require.NotEmpty(t, kind, "%T names no kind", req.GetType())
		produced[auditOrderTypeOfKind[kind]] = true
	}

	for kind, token := range auditOrderTypeOfKind {
		require.True(t, produced[token], "no request produces kind %q", kind)
	}
}

// With the committed order types known, a filter mixing an order-type leaf
// with another field is decided in one pass: an Or holds only if one of its
// arms does.
func TestAuditEntryMatchesMixedOrInOnePass(t *testing.T) {
	t.Parallel()

	ledgerB := auditEntry{seq: 10, ledgers: []string{"b"}, orderCount: 1, minLog: 40, maxLog: 40}
	metadataOnly := map[string]bool{"add_metadata": true}
	mixed := filterOr(
		filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "a"),
		filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction"),
	)

	require.False(t, auditEntryMatches(mixed, ledgerB, metadataOnly), "neither arm holds")
	require.True(t, auditEntryMatches(mixed, ledgerB, map[string]bool{"create_transaction": true}), "the order-type arm holds")

	ledgerA := ledgerB
	ledgerA.ledgers = []string{"a"}
	require.True(t, auditEntryMatches(mixed, ledgerA, metadataOnly), "the ledger arm holds")
	require.True(t, auditEntryMatches(mixed, ledgerB, nil), "unknown orders leave the order-type arm holding")
}
