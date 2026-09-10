package main

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal"
	"github.com/formancehq/ledger/v3/tests/oracle"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

const logTestPageSize = 8

// servedRows renders the model's own view of the given ids as a served page,
// so a test can then vary one field at a time.
func servedRows(ls oracle.LedgerState, ledger string, ids ...uint64) []serverLogRow {
	byID := map[uint64]oracle.LogRow{}
	for _, row := range ls.LogRows() {
		byID[row.ID] = row
	}

	out := make([]serverLogRow, 0, len(ids))

	for _, id := range ids {
		row := byID[id]
		out = append(out, serverLogRow{
			ledger:       ledger,
			id:           id,
			kind:         row.Kind,
			payload:      row.Payload,
			date:         row.Date.GetData(),
			hasDate:      row.Date != nil,
			sequence:     row.Sequence,
			purged:       row.PurgedVolumes,
			newKept:      row.NewKeptVolumes,
			ephemeral:    row.EphemeralVolumes,
			volumesKnown: true,
		})
	}

	return out
}

func filterLogDateLeaf() *commonpb.QueryFilter {
	min := uint64(1)

	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogBuiltinUint{
		LogBuiltinUint: &commonpb.LogBuiltinUintCondition{
			Field: commonpb.LogBuiltinIndex_LOG_BUILTIN_INDEX_DATE,
			Cond:  &commonpb.UintCondition{Min: &min},
		},
	}}
}

func filterLogIDLeaf() *commonpb.QueryFilter {
	min := uint64(1)

	return &commonpb.QueryFilter{Filter: &commonpb.QueryFilter_LogId{
		LogId: &commonpb.LogIdCondition{Cond: &commonpb.UintCondition{Min: &min}},
	}}
}

func reasonErr(t *testing.T, code codes.Code, reason string) error {
	t.Helper()

	st, err := status.New(code, reason).WithDetails(&errdetails.ErrorInfo{Domain: "ledger", Reason: reason})
	require.NoError(t, err)

	return st.Err()
}

// The date leaf is the only LOGS leaf served from an index, wherever it sits in
// the tree; every other shape needs none.
func TestNeededLogIndexes(t *testing.T) {
	t.Parallel()

	require.Equal(t, map[string]struct{}{logDateIndexCanonical: {}}, neededLogIndexes(filterLogDateLeaf()))
	require.Equal(t, map[string]struct{}{logDateIndexCanonical: {}},
		neededLogIndexes(filterOr(filterLogIDLeaf(), filterLogDateLeaf())), "a nested date leaf still needs the index")
	require.Nil(t, neededLogIndexes(filterLogIDLeaf()))
	require.Nil(t, neededLogIndexes(nil), "the unfiltered universe scan needs no index")
}

// Both index-gate rejections must reach the validator. INDEX_BUILDING rides
// codes.Unavailable and therefore sits inside internal.IsTransient: classifying
// it before the transient bail is what keeps a wrongly gated read a finding
// instead of a skipped blip.
func TestClassifyLogQueryError(t *testing.T) {
	t.Parallel()

	kind, gated := classifyLogQueryError(nil)
	require.True(t, gated)
	require.Equal(t, indexedErrNone, kind)

	notFound := reasonErr(t, codes.FailedPrecondition, "INDEX_NOT_FOUND")
	kind, gated = classifyLogQueryError(notFound)
	require.True(t, gated)
	require.Equal(t, indexedErrNotReady, kind)

	building := reasonErr(t, codes.Unavailable, "INDEX_BUILDING")
	require.True(t, internal.IsTransient(building), "the trap: INDEX_BUILDING is inside the transient set")

	kind, gated = classifyLogQueryError(building)
	require.True(t, gated, "an index-building refusal must reach the validator, not the transient bail")
	require.Equal(t, indexedErrNotReady, kind)

	_, gated = classifyLogQueryError(status.Error(codes.Unavailable, "connection refused"))
	require.False(t, gated, "a plain Unavailable stays environmental")

	_, gated = classifyLogQueryError(status.Error(codes.FailedPrecondition, "boom"))
	require.False(t, gated, "a reasonless precondition error is not an index-gate outcome")
}

// With the log-date index absent from the model, the refusal is REQUIRED, not
// merely tolerated: a page is a finding even when its rows are exactly the
// predicted window, which is what a server that stopped gating date filters
// would return.
func TestLogOutcome_AbsentDateIndexRequiresTheRefusal(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")
	filter := filterLogDateLeaf()
	needed := neededLogIndexes(filter)
	window := logWindow(ls, "L", filter, 0, logTestPageSize)

	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, servedRows(ls, "L", window...), 0, logTestPageSize),
		"a page served from an index no base holds is a finding, matching rows or not")
	require.True(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNotReady, nil, 0, logTestPageSize),
		"the not-ready refusal is the legal outcome while the index is absent")
}

// Once the model holds the index active the obligation flips: the page is owed,
// its rows are checked, and a refusal is a finding.
func TestLogOutcome_ActiveDateIndexRequiresThePage(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5), oracletest.CreateIndexReq(logDateIndexID()))
	gs.SetIndexActive("L", logDateIndexCanonical)

	ls := gs.Ledger("L")
	filter := filterLogDateLeaf()
	needed := neededLogIndexes(filter)
	window := logWindow(ls, "L", filter, 0, logTestPageSize)

	require.True(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, servedRows(ls, "L", window...), 0, logTestPageSize))
	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, servedRows(ls, "L", append(window, 999)...), 0, logTestPageSize),
		"a page whose rows are not the base's window is a finding")
	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNotReady, nil, 0, logTestPageSize),
		"refusing an index every base holds active is a finding")
}

// A log whose date this base has not learned cannot be decided against a date
// leaf — the shape a candidate base holds after folding an observed-but-
// undrained bulk — so the page may carry it or not, but nothing else.
func TestLogWindowMatches_UnlearnedDateIsOptional(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")
	filter := filterLogDateLeaf()

	rows := ls.LogDates()
	require.Len(t, rows, 1)
	require.Nil(t, rows[0].Date, "the fixture must leave the date unlearned")

	id := rows[0].ID

	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, nil),
		"an undecided row may be absent from the page")
	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, servedRows(ls, "L", id)),
		"and it may be present")
	require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, servedRows(ls, "L", id+7)),
		"a row the base does not hold at all is never legal")
}

// Once the date is learned the row is decided: a matching log is required, so a
// page with room that omits it is a finding.
func TestLogWindowMatches_LearnedDateIsRequired(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5))

	id := gs.Ledger("L").LogDates()[0].ID
	gs.LearnLogDate("L", id, &commonpb.Timestamp{Data: 5})

	ls := gs.Ledger("L")
	filter := filterLogDateLeaf() // date >= 1

	require.Equal(t, []uint64{id}, logWindow(ls, "L", filter, 0, logTestPageSize))
	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, servedRows(ls, "L", id)))
	require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, nil),
		"a page with room may not omit a required row")
}

// A page is compared field by field, not by id: the id can be right while the
// ledger, the payload kind or the server-assigned date is not. Each of those
// is a value the model derives or learned, so a mismatch is the server's.
func TestLogWindowMatches_ComparesEveryPinnedField(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5))

	id := gs.Ledger("L").LogRows()[0].ID
	gs.LearnLogDate("L", id, &commonpb.Timestamp{Data: 5})
	gs.LearnLogSequence("L", id, 12)

	ls := gs.Ledger("L")
	filter := filterLogIDLeaf()

	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, servedRows(ls, "L", id)),
		"the model's own view of the row is what the server owes")

	for _, tc := range []struct {
		name   string
		break_ func(*serverLogRow)
	}{
		{"another ledger's name", func(r *serverLogRow) { r.ledger = "other" }},
		{"another payload kind", func(r *serverLogRow) { r.kind = "deleted_metadata" }},
		{"a date the server never assigned", func(r *serverLogRow) { r.date = 6 }},
		{"no date at all", func(r *serverLogRow) { r.hasDate = false }},
		{"another global sequence", func(r *serverLogRow) { r.sequence = 77 }},
		{"a purged volume the bulk never drained", func(r *serverLogRow) { r.purged = "acc:1:USD/2" }},
		{"a new-kept volume the log did not touch", func(r *serverLogRow) { r.newKept = "acc:9:USD/2" }},
		{"no new-kept volumes at all", func(r *serverLogRow) { r.newKept = "" }},
		{"an ephemeral volume that outlived the bulk", func(r *serverLogRow) { r.ephemeral = "acc:1:USD/2" }},
		{"new-kept volumes out of order", func(r *serverLogRow) { r.newKept = "world:USD/2,acc:1:USD/2" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			page := servedRows(ls, "L", id)
			tc.break_(&page[0])

			require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, page))
		})
	}
}

// Before the model learns a date it cannot hold the server to one, so the row
// stays optional and its date is not compared — but its kind still is.
func TestLogWindowMatches_UnlearnedDateSkipsOnlyTheDate(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")

	rows := ls.LogRows()
	require.Len(t, rows, 1)
	require.Nil(t, rows[0].Date)

	filter := filterLogIDLeaf()

	page := servedRows(ls, "L", rows[0].ID)
	page[0].date, page[0].hasDate = 999, true
	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, page),
		"an unlearned date says nothing about the served one")

	page[0].kind = "drop_index"
	require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, page),
		"the kind is derived from the request and is compared regardless")
}

// A config-mutation log's payload is compared canonically: the model renders
// what the request implies and the served log must render the same. A right id
// and kind with the wrong key, type or value is a finding.
func TestLogWindowMatches_ComparesCanonicalPayload(t *testing.T) {
	t.Parallel()

	acct := commonpb.TargetType_TARGET_TYPE_ACCOUNT

	gs := buildGlobal(t, oracletest.SetFieldTypeReq(acct, "tier", commonpb.MetadataType_METADATA_TYPE_STRING))
	ls := gs.Ledger("L")

	rows := ls.LogRows()
	require.Len(t, rows, 1)
	require.Equal(t, "set_metadata_field_type", rows[0].Kind)
	require.NotEmpty(t, rows[0].Payload, "a schema log's payload is pinned")

	filter := filterLogIDLeaf()

	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, servedRows(ls, "L", rows[0].ID)))

	for _, tc := range []struct {
		name    string
		payload string
	}{
		{"another key", "target=1|key=grade|type=1"},
		{"another declared type", "target=1|key=tier|type=2"},
		{"another target", "target=2|key=tier|type=1"},
		{"no payload at all", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			page := servedRows(ls, "L", rows[0].ID)
			page[0].payload = tc.payload

			require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, page))
		})
	}
}

// A metadata log renders its target and its key-sorted, type-tagged values, so
// a value served under the wrong type or a key the request never carried is a
// finding.
func TestCanonicalServedLogPayload_MetadataValuesAreTypeTagged(t *testing.T) {
	t.Parallel()

	served := oracle.CanonicalServedLogPayload(&commonpb.LedgerLogPayload{
		Payload: &commonpb.LedgerLogPayload_SavedMetadata{SavedMetadata: &commonpb.SavedMetadata{
			Target: &commonpb.Target{Target: &commonpb.Target_Account{Account: &commonpb.TargetAccount{Addr: "acc:1"}}},
			Metadata: map[string]*commonpb.MetadataValue{
				"b": {Type: &commonpb.MetadataValue_StringValue{StringValue: "5"}},
				"a": {Type: &commonpb.MetadataValue_IntValue{IntValue: 5}},
			},
		}},
	})

	require.Equal(t, "target=acct:acc:1|a=i:5,b=s:5", served,
		"keys sorted, values type-tagged, so a string 5 and an int 5 never compare equal")
}

// The commit fold is where both server-assigned fields enter the model: a
// response's log carries the date and the global sequence, and learnTxStamps
// is what records them. Without this the read comparison has nothing to hold
// the server to.
func TestLearnTxStamps_RecordsLogDateAndSequence(t *testing.T) {
	t.Parallel()

	bulk := bulkOf(oracletest.TxReq("world", "acc:1", "USD/2", 5))

	res := oracle.NewGlobalState().Apply(bulk)
	require.True(t, res.OK, "setup bulk rejected: %s", res.Reason)

	gs := res.State
	row := gs.Ledger("L").LogRows()[0]
	require.Nil(t, row.Date)
	require.Zero(t, row.Sequence)

	learnTxStamps(gs, bulk, []*commonpb.Log{{
		Sequence: 17,
		Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{
			LedgerName: "L",
			Log:        &commonpb.LedgerLog{Id: row.ID, Date: &commonpb.Timestamp{Data: 99}},
		}}},
	}})

	learned := gs.Ledger("L").LogRows()[0]
	assert.Equal(t, uint64(99), learned.Date.GetData(), "the date comes from the commit response")
	assert.Equal(t, uint64(17), learned.Sequence, "so does the global sequence")
}

// The global sequence spans every ledger and the technical entries between
// them, so the model holds it only for a bulk whose response it folded. Until
// then the served value is not compared; once learned it is.
func TestLogWindowMatches_GlobalSequenceComparedOnceLearned(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5))
	id := gs.Ledger("L").LogRows()[0].ID

	ls := gs.Ledger("L")
	filter := filterLogIDLeaf()
	require.Zero(t, ls.LogRows()[0].Sequence, "the fixture must leave the sequence unlearned")

	page := servedRows(ls, "L", id)
	page[0].sequence = 4242
	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, page),
		"an unlearned sequence says nothing about the served one")

	gs.LearnLogSequence("L", id, 12)
	ls = gs.Ledger("L")

	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, servedRows(ls, "L", id)))
	require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, page),
		"a served sequence other than the learned one is a finding")
}

// A page may be truncated but never gap-toothed: carrying a later required row
// while skipping an earlier one is a finding, which is what a server serving
// the wrong version's keyspace would return.
func TestLogWindowMatches_RequiredRowMayNotBeSkipped(t *testing.T) {
	t.Parallel()

	gs := buildGlobal(t,
		oracletest.TxReq("world", "acc:1", "USD/2", 5),
		oracletest.TxReq("world", "acc:2", "USD/2", 7),
	)

	rows := gs.Ledger("L").LogDates()
	require.Len(t, rows, 2)

	for _, row := range rows {
		gs.LearnLogDate("L", row.ID, &commonpb.Timestamp{Data: 5})
	}

	ls := gs.Ledger("L")
	filter := filterLogDateLeaf() // date >= 1: both rows required

	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, servedRows(ls, "L", rows[0].ID, rows[1].ID)))
	require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, servedRows(ls, "L", rows[1].ID)),
		"skipping an earlier required row is a finding")
	require.True(t, logWindowMatches(ls, "L", filter, 0, 1, servedRows(ls, "L", rows[0].ID)),
		"a full page of one legitimately truncates the rest")
	require.False(t, logWindowMatches(ls, "L", filter, 0, 1, servedRows(ls, "L", rows[1].ID)),
		"a full page must still start at the first required row")
}

// The generator churns the log-date index, so the LOGS date filters cycle
// through absent, ambiguous and active instead of only ever being refused.
func TestWorkloadIndexes_IncludesTheLogDateIndex(t *testing.T) {
	t.Parallel()

	var found bool
	for _, wi := range workloadIndexes() {
		if wi.canonical == logDateIndexCanonical {
			found = true
		}
	}

	require.True(t, found, "the log-date index must be in the create/drop pool")
}

// A filter needing no index may never be gated.
func TestLogOutcome_IndexFreeFilterMayNotBeGated(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")
	filter := filterLogIDLeaf()
	needed := neededLogIndexes(filter)
	window := logWindow(ls, "L", filter, 0, logTestPageSize)

	require.NotEmpty(t, window, "the id filter must select the committed log, or this pins nothing")
	require.True(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, servedRows(ls, "L", window...), 0, logTestPageSize))
	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNone, nil, 0, logTestPageSize),
		"an empty page where the model holds rows is a finding")
	require.False(t, logOutcomeLegal(ls, "L", filter, needed, indexedErrNotReady, nil, 0, logTestPageSize),
		"a not-ready refusal of an index-free filter is a finding")
}

// serverLogRows must read the three volume annotations off the wire in the
// order the FSM wrote them, and a colour — a dimension the model's volume key
// does not carry — must take the whole comparison off rather than fail it.
func TestServerLogRows_ReadsVolumeAnnotations(t *testing.T) {
	t.Parallel()

	logOf := func(l *commonpb.LedgerLog) *commonpb.Log {
		return &commonpb.Log{Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_Apply{
			Apply: &commonpb.ApplyLedgerLog{LedgerName: "L", Log: l},
		}}}
	}

	rows := serverLogRows([]*commonpb.Log{logOf(&commonpb.LedgerLog{
		Id:               1,
		PurgedVolumes:    []*commonpb.TouchedVolume{{Account: "e:1", Asset: "USD"}},
		NewKeptVolumes:   []*commonpb.TouchedVolume{{Account: "world", Asset: "EUR"}, {Account: "n:1", Asset: "EUR"}},
		EphemeralVolumes: []*commonpb.TouchedVolume{{Account: "e:2", Asset: "USD"}},
	})})

	require.Len(t, rows, 1)
	require.True(t, rows[0].volumesKnown)
	require.Equal(t, "e:1:USD", rows[0].purged)
	require.Equal(t, "world:EUR,n:1:EUR", rows[0].newKept,
		"read verbatim: the model renders its own list sorted, so a mis-sorted served list must not be repaired here")
	require.Equal(t, "e:2:USD", rows[0].ephemeral)

	coloured := serverLogRows([]*commonpb.Log{logOf(&commonpb.LedgerLog{
		Id:             1,
		NewKeptVolumes: []*commonpb.TouchedVolume{{Account: "n:1", Asset: "EUR", Color: "red"}},
	})})

	require.Len(t, coloured, 1)
	require.False(t, coloured[0].volumesKnown, "a colour splits a cell the model holds as one")
}

// With a colour on the wire the three annotations say nothing, so a page that
// disagrees on them is still legal — every other field is still compared.
func TestLogWindowMatches_ColouredVolumesAreNotCompared(t *testing.T) {
	t.Parallel()

	ls := buildGlobal(t, oracletest.TxReq("world", "acc:1", "USD/2", 5)).Ledger("L")
	filter := filterLogIDLeaf()
	id := ls.LogRows()[0].ID

	page := servedRows(ls, "L", id)
	page[0].newKept, page[0].volumesKnown = "", false

	require.True(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, page))

	page[0].kind = "drop_index"
	require.False(t, logWindowMatches(ls, "L", filter, 0, logTestPageSize, page))
}
