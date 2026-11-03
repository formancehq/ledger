package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

const numscriptTestContent = "send [USD 1] (source = @world destination = @x)"

func saveNumscriptOrder(ledger, name, content, version string) *raftcmdpb.Order {
	return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
		Ledger:  ledger,
		Payload: &raftcmdpb.LedgerScopedOrder_SaveNumscript{SaveNumscript: &raftcmdpb.SaveNumscriptOrder{Name: name, Content: content, Version: version}},
	}}}
}

func tamperNumscriptContent(t *testing.T, e *testEngine, info *commonpb.NumscriptInfo) {
	t.Helper()

	batch := e.store.OpenWriteSession()
	key := domain.NumscriptEntryKey{LedgerName: info.GetLedger(), Name: info.GetName(), Version: info.GetVersion()}
	_, err := e.attrs.NumscriptContent.Set(batch, key.Bytes(), info)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func dropNumscriptContent(t *testing.T, e *testEngine, ledger, name, version string) {
	t.Helper()

	batch := e.store.OpenWriteSession()
	key := domain.NumscriptEntryKey{LedgerName: ledger, Name: name, Version: version}
	require.NoError(t, e.attrs.NumscriptContent.Delete(batch, key.Bytes()))
	require.NoError(t, batch.Commit())
}

func tamperNumscriptLatest(t *testing.T, e *testEngine, ledger, name, version string) {
	t.Helper()

	batch := e.store.OpenWriteSession()
	key := domain.NumscriptVersionKey{LedgerName: ledger, Name: name}
	_, err := e.attrs.NumscriptVersion.Set(batch, key.Bytes(), &commonpb.NumscriptVersionValue{Version: version})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

func numscriptMismatches(errs []*servicepb.CheckStoreError) []*servicepb.CheckStoreError {
	var out []*servicepb.CheckStoreError
	for _, e := range errs {
		if e.GetErrorType() == servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_NUMSCRIPT_MISMATCH {
			out = append(out, e)
		}
	}

	return out
}

// TestCheckerNumscript_CleanProjectionPasses: saves with their matching
// projection (greatest-semver pointer) yield no numscript mismatch.
func TestCheckerNumscript_CleanProjectionPasses(t *testing.T) {
	t.Parallel()

	e := newTestEngine(t)
	e.processAndCommit(createLedgerOrder("main"))
	// Save out of order: pointer must end at the greatest (2.0.0).
	e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "2.0.0"))
	e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "1.0.0"))

	require.Empty(t, numscriptMismatches(collectCheckErrors(t, e.store, e.attrs)))
}

// TestCheckerNumscript_ContentTamperDetected: altered stored content diverges
// from the audit-derived content.
func TestCheckerNumscript_ContentTamperDetected(t *testing.T) {
	t.Parallel()

	e := newTestEngine(t)
	e.processAndCommit(createLedgerOrder("main"))
	logs := e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "1.0.0"))

	tampered := logs[0].GetPayload().GetSavedNumscript().GetInfo().CloneVT()
	tampered.Content = "send [USD 999] (source = @world destination = @evil)"
	tamperNumscriptContent(t, e, tampered)

	require.NotEmpty(t, numscriptMismatches(collectCheckErrors(t, e.store, e.attrs)))
}

// TestCheckerNumscript_PointerDriftDetected: the stored latest pointer is not
// the greatest saved semver.
func TestCheckerNumscript_PointerDriftDetected(t *testing.T) {
	t.Parallel()

	e := newTestEngine(t)
	e.processAndCommit(createLedgerOrder("main"))
	e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "1.0.0"))
	e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "2.0.0"))

	// Audit-derived greatest is 2.0.0; roll the pointer back to 1.0.0.
	tamperNumscriptLatest(t, e, "main", "pay", "1.0.0")

	require.NotEmpty(t, numscriptMismatches(collectCheckErrors(t, e.store, e.attrs)))
}

// TestCheckerNumscript_MissingContentDetected: an immutable content entry the
// SavedNumscript log requires is gone.
func TestCheckerNumscript_MissingContentDetected(t *testing.T) {
	t.Parallel()

	e := newTestEngine(t)
	e.processAndCommit(createLedgerOrder("main"))
	e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "1.0.0"))

	dropNumscriptContent(t, e, "main", "pay", "1.0.0")

	require.NotEmpty(t, numscriptMismatches(collectCheckErrors(t, e.store, e.attrs)))
}

// TestCheckerNumscript_ExtraContentDetected: a stored content row that no
// SavedNumscript log accounts for.
func TestCheckerNumscript_ExtraContentDetected(t *testing.T) {
	t.Parallel()

	e := newTestEngine(t)
	e.processAndCommit(createLedgerOrder("main"))

	tamperNumscriptContent(t, e, &commonpb.NumscriptInfo{Name: "ghost", Version: "1.0.0", Content: numscriptTestContent, Ledger: "main"})

	require.NotEmpty(t, numscriptMismatches(collectCheckErrors(t, e.store, e.attrs)))
}

func TestCheckerNumscript_LatestPointerPresence(t *testing.T) {
	t.Parallel()

	for _, scenario := range []string{"missing", "surplus", "empty history"} {
		t.Run(scenario, func(t *testing.T) {
			t.Parallel()

			e := newTestEngine(t)
			if scenario != "empty history" {
				e.processAndCommit(createLedgerOrder("main"))
			}
			require.Empty(t, collectCheckErrors(t, e.store, e.attrs))

			var want string
			if scenario == "missing" {
				e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "1.0.0"))
				require.Empty(t, collectCheckErrors(t, e.store, e.attrs))
				batch := e.store.OpenWriteSession()
				key := domain.NumscriptVersionKey{LedgerName: "main", Name: "pay"}
				require.NoError(t, e.attrs.NumscriptVersion.Delete(batch, key.Bytes()))
				require.NoError(t, batch.Commit())
				want = `audit chain expects a numscript latest pointer for "pay" on ledger "main" but the store has no matching entry`
			} else {
				tamperNumscriptLatest(t, e, "main", "pay", "1.0.0")
				want = `stored numscript latest pointer for "pay" on ledger "main" has no matching SavedNumscript in the audit chain`
			}

			got := collectCheckErrors(t, e.store, e.attrs)
			require.Len(t, got, 1)
			require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_NUMSCRIPT_MISMATCH, got[0].GetErrorType())
			require.Equal(t, "main", got[0].GetLedger())
			require.Equal(t, uint64(0), got[0].GetLogSequence())
			require.Equal(t, want, got[0].GetMessage())
		})
	}
}

func TestCheckerNumscript_LedgerDeletion(t *testing.T) {
	t.Parallel()

	e := newTestEngine(t)
	e.processAndCommit(createLedgerOrder("main"), createLedgerOrder("other"))
	logs := e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "2.0.0"))
	e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "1.0.0"))
	e.processAndCommit(saveNumscriptOrder("main", "refund", numscriptTestContent, "1.0.0"))
	e.processAndCommit(saveNumscriptOrder("other", "pay", numscriptTestContent, "1.0.0"))
	require.Empty(t, collectCheckErrors(t, e.store, e.attrs))

	e.processAndCommit(deleteLedgerOrder("main"))
	// Deletion removes every version and name for main while retaining other.
	require.Empty(t, collectCheckErrors(t, e.store, e.attrs))

	// Restore a deleted content row and pointer without changing the audit.
	tamperNumscriptContent(t, e, logs[0].GetPayload().GetSavedNumscript().GetInfo())
	tamperNumscriptLatest(t, e, "main", "pay", "2.0.0")
	got := collectCheckErrors(t, e.store, e.attrs)
	require.Len(t, got, 2)
	for _, finding := range got {
		require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_NUMSCRIPT_MISMATCH, finding.GetErrorType())
		require.Equal(t, "main", finding.GetLedger())
		require.Equal(t, uint64(0), finding.GetLogSequence())
	}
	require.Equal(t, `stored numscript content "pay"@"2.0.0" for ledger "main" survives a replayed DeleteLedger + completed cleanup`, got[0].GetMessage())
	require.Equal(t, `stored numscript latest pointer for "pay" on ledger "main" survives a replayed DeleteLedger + completed cleanup`, got[1].GetMessage())
}

func TestCheckerNumscript_CoordinatedLogAndContentTamper(t *testing.T) {
	t.Parallel()

	e := newTestEngine(t)
	e.processAndCommit(createLedgerOrder("main"))
	logs := e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "1.0.0"))
	require.Empty(t, collectCheckErrors(t, e.store, e.attrs))

	// Make the stored log and content agree with each other while preserving
	// the audited save order. Expectations must still come from that order.
	forgedLog := logs[0].CloneVT()
	info := forgedLog.GetPayload().GetSavedNumscript().GetInfo()
	info.Content = "send [USD 999] (source = @world destination = @evil)"
	batch := e.store.OpenWriteSession()
	key := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(forgedLog.GetSequence()).Build()
	require.NoError(t, batch.SetProto(key, forgedLog))
	require.NoError(t, batch.Commit())
	tamperNumscriptContent(t, e, info)

	got := collectCheckErrors(t, e.store, e.attrs)
	require.Len(t, got, 2)
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_LOG_PAYLOAD_MISMATCH, got[0].GetErrorType())
	require.Equal(t, forgedLog.GetSequence(), got[0].GetLogSequence())
	require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_NUMSCRIPT_MISMATCH, got[1].GetErrorType())
	require.Equal(t, "main", got[1].GetLedger())
	require.Equal(t, `numscript "pay"@"1.0.0" for ledger "main": stored entry diverges from the audit-derived NumscriptInfo`, got[1].GetMessage())
}

func TestCompareNumscripts_ExplicitExpectations(t *testing.T) {
	t.Parallel()

	e := newTestEngine(t)
	e.processAndCommit(createLedgerOrder("main"))
	logs := e.processAndCommit(saveNumscriptOrder("main", "pay", numscriptTestContent, "1.0.0"))
	require.Empty(t, collectCheckErrors(t, e.store, e.attrs))

	reader, err := e.store.NewReadHandle()
	require.NoError(t, err)
	defer func() { require.NoError(t, reader.Close()) }()

	// Explicit expectations are authoritative for this comparison interface;
	// it must not reconstruct them from either the stored rows or the audit.
	var got []*servicepb.CheckStoreError
	checker := NewChecker(e.store, e.attrs, nil, logging.Testing())
	checker.compareNumscripts(reader,
		map[domain.NumscriptEntryKey]*commonpb.NumscriptInfo{
			{LedgerName: "main", Name: "pay", Version: "1.0.0"}: logs[0].GetPayload().GetSavedNumscript().GetInfo(),
		},
		map[domain.NumscriptVersionKey]string{
			{LedgerName: "main", Name: "pay"}: "1.0.0",
		}, nil, func(event *servicepb.CheckStoreEvent) {
			got = append(got, event.GetError())
		})
	require.Empty(t, got)

	checker.compareNumscripts(reader, nil, nil, nil, func(event *servicepb.CheckStoreEvent) {
		got = append(got, event.GetError())
	})
	require.Len(t, got, 2)
	for _, finding := range got {
		require.Equal(t, servicepb.CheckStoreErrorType_CHECK_STORE_ERROR_TYPE_NUMSCRIPT_MISMATCH, finding.GetErrorType())
		require.Equal(t, "main", finding.GetLedger())
	}
	require.Equal(t, `stored numscript content "pay"@"1.0.0" for ledger "main" has no matching SavedNumscript in the audit chain`, got[0].GetMessage())
	require.Equal(t, `stored numscript latest pointer for "pay" on ledger "main" has no matching SavedNumscript in the audit chain`, got[1].GetMessage())
}
