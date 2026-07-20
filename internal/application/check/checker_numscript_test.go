package check

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
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
