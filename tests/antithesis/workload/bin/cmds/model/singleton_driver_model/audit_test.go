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

func TestAuditSuccessMismatchPinsOneBulk(t *testing.T) {
	t.Parallel()

	logs := map[uint64]committedLog{
		10: {ledger: "a", id: 1}, 11: {ledger: "b", id: 1}, 12: {ledger: "a", id: 2},
	}
	entry := auditEntry{seq: 7, ledgers: []string{"a", "b"}, orderCount: 3, minLog: 10, maxLog: 12, itemSeqs: []uint64{10, 11, 12}}

	why, _ := auditSuccessMismatch(entry, logs, 12)
	require.Empty(t, why)

	archived := entry
	archived.itemSeqs = nil
	why, _ = auditSuccessMismatch(archived, logs, 12)
	require.Empty(t, why, "purged items are not a mismatch")

	e := entry
	e.ledgers = []string{"a"}
	why, _ = auditSuccessMismatch(e, logs, 12)
	require.Contains(t, why, "entry names a")

	e = entry
	e.orderCount = 2
	why, _ = auditSuccessMismatch(e, logs, 12)
	require.Contains(t, why, "order count")

	e = entry
	e.maxLog = 13
	why, _ = auditSuccessMismatch(e, logs, 12)
	require.Contains(t, why, "straddles")

	e = entry
	e.itemSeqs = []uint64{10, 11, 99}
	why, _ = auditSuccessMismatch(e, logs, 12)
	require.Contains(t, why, "outside the entry")

	delete(logs, 11)
	why, _ = auditSuccessMismatch(entry, logs, 12)
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
	require.Empty(t, auditPageViolation(page, 2, true, ledgerA))
	require.Equal(t, "page longer than requested", auditPageViolation(page, 1, true, ledgerA))
	require.Equal(t, "ascending order violated", auditPageViolation(page, 2, false, ledgerA))
	require.Equal(t, "entry outside the filter", auditPageViolation(page, 2, true, filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "b")))
	require.Equal(t, "entry outside the filter", auditPageViolation(page, 2, true, filterAuditString(commonpb.AuditField_AUDIT_FIELD_OUTCOME, "failure")))
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

	require.Empty(t, auditPageViolation(page(1, 2, 3), 10, false, nil))
	require.Equal(t, "gap in a zone-scan page", auditPageViolation(page(1, 3), 10, false, nil))
	require.Equal(t, "zone-scan page does not start at its lower bound", auditPageViolation(page(2, 3), 10, false, nil))

	lo := uint64(2)
	from2 := filterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo})
	require.Empty(t, auditPageViolation(page(2, 3), 10, false, from2))
	require.Empty(t, auditPageViolation(page(3, 4), 10, false, filterAuditUint(commonpb.AuditField_AUDIT_FIELD_SEQUENCE, &commonpb.UintCondition{Min: &lo, MinExclusive: true})), "an exclusive bound starts one past it")
	require.Equal(t, "zone-scan page does not start at its lower bound", auditPageViolation(page(1, 2), 10, false, from2))
	require.Empty(t, auditPageViolation(page(5, 4, 3), 10, true, nil), "a reverse scan starts anywhere but stays dense")
	require.Equal(t, "gap in a zone-scan page", auditPageViolation(page(5, 3), 10, true, nil))

	indexed := filterAnd(from2, filterAuditString(commonpb.AuditField_AUDIT_FIELD_LEDGER, "a"))
	require.Empty(t, auditPageViolation(page(2, 5, 9), 10, false, indexed), "an indexed page selects sparsely")
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
	}
	require.Len(t, orders, len(auditOrderTypeOfKind))
	for kind, order := range orders {
		require.Equal(t, domain.AuditOrderType(order), auditOrderTypeOfKind[kind], kind)
	}

	require.Contains(t, auditOrderTypes, domain.AuditOrderType(ledgerScoped(&raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_SaveLedgerMetadata{}})))
	require.Contains(t, auditOrderTypes, domain.AuditOrderType(ledgerScoped(&raftcmdpb.LedgerScopedOrder{Payload: &raftcmdpb.LedgerScopedOrder_DeleteLedgerMetadata{}})))
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
	c := &Checker{ledgerNames: []string{"L"}, modelState: committedStateWithSequences(t, 42, 44), inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]string{}, rejections: map[rejectedBulk]struct{}{}}
	e42 := auditEntry{seq: 3, ledgers: []string{"L"}, orderCount: 1, minLog: 42, maxLog: 42}
	e44 := auditEntry{seq: 5, ledgers: []string{"L"}, orderCount: 1, minLog: 44, maxLog: 44}

	addMeta := filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "add_metadata")
	createTx := filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "create_transaction")
	require.Empty(t, c.validateAuditPage(0, []auditEntry{e42}, false, addMeta, 50).finding)
	require.Equal(t, "audit entry outside the order-type scope", c.validateAuditPage(0, []auditEntry{e42}, false, createTx, 50).finding)

	c.ledgerLogSeqs[43] = "L"
	e43 := auditEntry{seq: 4, ledgers: []string{"L"}, orderCount: 1, minLog: 43, maxLog: 43}
	require.Empty(t, c.validateAuditPage(0, []auditEntry{e43}, false, filterAuditString(commonpb.AuditField_AUDIT_FIELD_ORDER_TYPE, "delete_ledger_metadata"), 50).finding, "a ledger-level log may be either ledger-metadata order")

	lo, hi := uint64(40), uint64(50)
	logRange := filterAuditUint(commonpb.AuditField_AUDIT_FIELD_LOG_SEQUENCE, &commonpb.UintCondition{Min: &lo, Max: &hi})
	require.Empty(t, c.validateAuditPage(0, []auditEntry{e42, e43, e44}, false, logRange, 50).finding)
	require.Equal(t, "audit trail misses a committed log", c.validateAuditPage(0, []auditEntry{e42, e44}, false, logRange, 50).finding, "43 is committed and in range")
	require.Empty(t, c.validateAuditPage(0, []auditEntry{e42, e44}, false, logRange, 2).finding, "a full page may have been cut before 43")
}

// A reverse first page must reach the newest committed bulk unless the newest
// ledger-scoped entry is a rejection or a bulk is still in flight.
func TestValidateAuditPageReverseTail(t *testing.T) {
	t.Parallel()

	// Logs at 42 and 44; 43 is a hole inside the learned range.
	c := &Checker{ledgerNames: []string{"L"}, modelState: committedStateWithSequences(t, 42, 44), inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]string{}, rejections: map[rejectedBulk]struct{}{{ledgers: "L", orders: 1, reason: "VALIDATION"}: {}}}

	newest := auditEntry{seq: 5, ledgers: []string{"L"}, orderCount: 1, minLog: 44, maxLog: 44}
	older := auditEntry{seq: 3, ledgers: []string{"L"}, orderCount: 1, minLog: 42, maxLog: 42}
	hole := auditEntry{seq: 4, ledgers: []string{"L"}, orderCount: 1, minLog: 43, maxLog: 43}
	rejected := auditEntry{seq: 6, ledgers: []string{"L"}, orderCount: 1, failed: true, reason: "VALIDATION"}
	system := auditEntry{seq: 7}

	require.Empty(t, c.validateAuditPage(0, []auditEntry{newest, older}, true, nil, 50).finding)
	require.Equal(t, "audit entry outside model", c.validateAuditPage(0, []auditEntry{hole}, true, nil, 50).finding, "43 is no committed log")

	res := c.validateAuditPage(0, []auditEntry{rejected, newest}, true, nil, 50)
	require.Empty(t, res.finding)
	require.Equal(t, 1, res.rejections)

	require.Equal(t, "audit tail misses the newest committed bulk", c.validateAuditPage(0, []auditEntry{system, older}, true, nil, 50).finding)
	require.Empty(t, c.validateAuditPage(0, []auditEntry{rejected, older}, true, nil, 50).finding, "a newest rejection is the real tail")
	require.Empty(t, c.validateAuditPage(0, []auditEntry{older}, false, nil, 50).finding, "only a reverse first page starts at the newest entry")

	c.inflight[1] = oracle.Bulk{}
	require.Empty(t, c.validateAuditPage(1, []auditEntry{older}, true, nil, 50).finding, "an undrained bulk may be the real tail")
}

// Entries the driver never drained — setup's ledgers and initial schema — are
// not judged, and a ledger-level log's sequence is learned outside the oracle.
func TestValidateAuditPageSetupEraAndLedgerLevelLogs(t *testing.T) {
	t.Parallel()

	c := &Checker{ledgerNames: []string{"L"}, modelState: committedStateWithSequences(t, 42), inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]string{}}

	setup := auditEntry{seq: 1, ledgers: []string{"L"}, orderCount: 1, minLog: 2, maxLog: 2}
	require.Empty(t, c.validateAuditPage(0, []auditEntry{setup}, false, nil, 50).finding, "below the first learned sequence is setup")

	ledgerLevel := auditEntry{seq: 8, ledgers: []string{"L"}, orderCount: 1, minLog: 43, maxLog: 43}
	require.Equal(t, "audit entry names logs the model never committed", c.validateAuditPage(0, []auditEntry{ledgerLevel}, false, nil, 50).finding)

	c.ledgerLogSeqs[43] = "L"
	require.Empty(t, c.validateAuditPage(0, []auditEntry{ledgerLevel}, false, nil, 50).finding, "a learned ledger-level sequence is a committed log")
}

// The audit trail serves its whole history from the first page on, so a
// rejection or ledger-level log stays explainable however many newer ones the
// run records.
func TestAuditHistoryIsNeverPruned(t *testing.T) {
	t.Parallel()

	c := &Checker{rejections: map[rejectedBulk]struct{}{}, ledgerLogSeqs: map[uint64]string{}}
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
	require.Equal(t, "L", c.ledgerLogSeqs[1])
}

func saveLedgerMetaReqL(ledger string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SaveLedgerMetadata{
		SaveLedgerMetadata: &servicepb.SaveLedgerMetadataRequest{Ledger: ledger, Metadata: map[string]*commonpb.MetadataValue{"k": {Type: &commonpb.MetadataValue_StringValue{StringValue: "v"}}}},
	}}
}
