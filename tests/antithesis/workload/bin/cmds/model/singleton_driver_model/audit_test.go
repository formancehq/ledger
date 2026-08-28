package main

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
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

	page := []auditEntry{{seq: 3, ledgers: []string{"a"}}, {seq: 2, ledgers: []string{"a"}}}
	require.Empty(t, auditPageViolation(page, 2, true, "a", ""))
	require.Equal(t, "page longer than requested", auditPageViolation(page, 1, true, "", ""))
	require.Equal(t, "ascending order violated", auditPageViolation(page, 2, false, "", ""))
	require.Equal(t, "entry outside the ledger scope", auditPageViolation(page, 2, true, "b", ""))
	require.Equal(t, "success entry in a failure-scoped page", auditPageViolation(page, 2, true, "", "failure"))
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

	require.Empty(t, c.validateAuditPage(0, []auditEntry{newest, older}, true).finding)
	require.Equal(t, "audit entry outside model", c.validateAuditPage(0, []auditEntry{hole}, true).finding, "43 is no committed log")

	res := c.validateAuditPage(0, []auditEntry{rejected, newest}, true)
	require.Empty(t, res.finding)
	require.Equal(t, 1, res.rejections)

	require.Equal(t, "audit tail misses the newest committed bulk", c.validateAuditPage(0, []auditEntry{system, older}, true).finding)
	require.Empty(t, c.validateAuditPage(0, []auditEntry{rejected, older}, true).finding, "a newest rejection is the real tail")
	require.Empty(t, c.validateAuditPage(0, []auditEntry{older}, false).finding, "only a reverse first page starts at the newest entry")

	c.inflight[1] = oracle.Bulk{}
	require.Empty(t, c.validateAuditPage(1, []auditEntry{older}, true).finding, "an undrained bulk may be the real tail")
}

// Entries the driver never drained — setup's ledgers and initial schema — are
// not judged, and a ledger-level log's sequence is learned outside the oracle.
func TestValidateAuditPageSetupEraAndLedgerLevelLogs(t *testing.T) {
	t.Parallel()

	c := &Checker{ledgerNames: []string{"L"}, modelState: committedStateWithSequences(t, 42), inflight: map[uint64]oracle.Bulk{}, ledgerLogSeqs: map[uint64]string{}}

	setup := auditEntry{seq: 1, ledgers: []string{"L"}, orderCount: 1, minLog: 2, maxLog: 2}
	require.Empty(t, c.validateAuditPage(0, []auditEntry{setup}, false).finding, "below the first learned sequence is setup")

	ledgerLevel := auditEntry{seq: 8, ledgers: []string{"L"}, orderCount: 1, minLog: 43, maxLog: 43}
	require.Equal(t, "audit entry names logs the model never committed", c.validateAuditPage(0, []auditEntry{ledgerLevel}, false).finding)

	c.ledgerLogSeqs[43] = "L"
	require.Empty(t, c.validateAuditPage(0, []auditEntry{ledgerLevel}, false).finding, "a learned ledger-level sequence is a committed log")
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
