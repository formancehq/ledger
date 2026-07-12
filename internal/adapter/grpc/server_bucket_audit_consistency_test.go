package grpc

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
	"google.golang.org/protobuf/proto"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	internalauth "github.com/formancehq/ledger/v3/internal/adapter/auth"
	"github.com/formancehq/ledger/v3/internal/application/ctrl/ctrlmock"
	"github.com/formancehq/ledger/v3/internal/pkg/cursor"
	"github.com/formancehq/ledger/v3/internal/proto/auditpb"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
	"github.com/formancehq/ledger/v3/internal/storage/readstore"
)

// auditStream is a minimal streaming server whose Context is caller-controlled,
// so a test can cancel the request while a handler blocks in a wait. The
// fakeServerStream helper hardcodes context.Background(), which cannot be
// cancelled; the audit-consistency waits need a cancellable one.
func newAuditStream(t *testing.T, ctx context.Context) *fakeAuditStream {
	t.Helper()

	f := &fakeAuditStream{
		MockServerStreamingServer: NewMockServerStreamingServer[auditpb.AuditEntry](gomock.NewController(t)),
	}
	f.MockServerStreamingServer.EXPECT().Context().Return(ctx).AnyTimes()
	f.MockServerStreamingServer.EXPECT().Send(gomock.Any()).Return(nil).AnyTimes()
	f.MockServerStreamingServer.EXPECT().SetTrailer(gomock.Any()).AnyTimes()
	f.MockServerStreamingServer.EXPECT().SetHeader(gomock.Any()).Return(nil).AnyTimes()
	f.MockServerStreamingServer.EXPECT().SendHeader(gomock.Any()).Return(nil).AnyTimes()
	f.MockServerStreamingServer.EXPECT().SendMsg(gomock.Any()).Return(nil).AnyTimes()
	f.MockServerStreamingServer.EXPECT().RecvMsg(gomock.Any()).Return(nil).AnyTimes()

	return f
}

type fakeAuditStream struct {
	*MockServerStreamingServer[auditpb.AuditEntry]
}

// writeMainAuditEntry writes an audit entry into the main store's Cold/Audit
// zone so ReadLastAuditSequence observes it as the live audit head.
func writeMainAuditEntry(t *testing.T, store *dal.Store, seq uint64) {
	t.Helper()

	batch := store.OpenWriteSession()
	kb := dal.NewKeyBuilder()
	val, err := proto.Marshal(&auditpb.AuditEntry{
		Sequence: seq,
		Outcome:  &auditpb.AuditEntry_Success{Success: &auditpb.AuditSuccess{}},
	})
	require.NoError(t, err)
	require.NoError(t, batch.SetBytes(
		kb.PutZonePrefix(dal.ZoneCold, dal.SubColdAudit).PutUint64(seq).Build(),
		val,
	))
	require.NoError(t, batch.Commit())
}

// writeReadstoreLogProgress advances the log-index cursor so waitMinLogSequence
// is satisfied.
func writeReadstoreLogProgress(t *testing.T, rs *readstore.Store, seq uint64) {
	t.Helper()

	batch := rs.NewBatch()
	require.NoError(t, rs.WriteProgress(batch, seq))
	require.NoError(t, batch.Commit())
}

// writeReadstoreAuditProgress advances the audit-index cursor.
func writeReadstoreAuditProgress(t *testing.T, rs *readstore.Store, seq uint64) {
	t.Helper()

	batch := rs.NewBatch()
	require.NoError(t, rs.WriteAuditProgress(batch, seq))
	require.NoError(t, batch.Commit())
}

func newAuditConsistencyHarness(t *testing.T) (*BucketServiceServerImpl, *ctrlmock.MockController, *dal.Store, *readstore.Store) {
	t.Helper()

	logger := logging.FromContext(logging.TestingContext())
	meter := noop.NewMeterProvider().Meter("test")

	mainStore, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = mainStore.Close() })

	rs, err := readstore.New(t.TempDir(), logger, readstore.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = rs.Close() })

	mockCtrl := ctrlmock.NewMockController(gomock.NewController(t))

	impl := &BucketServiceServerImpl{
		logger:    noopLogger{},
		ctrl:      mockCtrl,
		store:     mainStore,
		readStore: rs,
		authCfg:   internalauth.AuthConfig{}, // disabled → Authenticate is a no-op
	}

	return impl, mockCtrl, mainStore, rs
}

// singleFieldFilter builds a minimal non-nil QueryFilter so the handler takes
// the filtered branch. Its content is irrelevant here: the controller is mocked,
// so the filter is never actually compiled — only its presence matters.
func singleFieldFilter() *commonpb.QueryFilter {
	return &commonpb.QueryFilter{
		Filter: &commonpb.QueryFilter_Field{Field: &commonpb.FieldCondition{}},
	}
}

// TestListAuditEntriesFilteredWaitsForAuditProgress verifies a live, filtered
// ListAuditEntries with min_log_sequence>0 blocks until the audit index catches
// up to the live audit head, even when the log index is already caught up.
func TestListAuditEntriesFilteredWaitsForAuditProgress(t *testing.T) {
	t.Parallel()

	impl, mockCtrl, mainStore, rs := newAuditConsistencyHarness(t)

	// Log index already caught up to the requested bound.
	writeReadstoreLogProgress(t, rs, 5)
	// Live audit head is at seq 9 (audit space diverges from log space).
	writeMainAuditEntry(t, mainStore, 9)
	// Audit index lags: cursor at 3.
	writeReadstoreAuditProgress(t, rs, 3)

	// The controller must only be called AFTER the audit index catches up.
	controllerCalled := make(chan struct{})
	mockCtrl.EXPECT().
		ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Not(gomock.Nil()), gomock.Any()).
		DoAndReturn(func(_ context.Context, _ uint32, _ uint64, _ *commonpb.QueryFilter, _ bool) (cursor.Cursor[*auditpb.AuditEntry], error) {
			close(controllerCalled)

			return cursor.NewSliceCursor([]*auditpb.AuditEntry(nil)), nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	handlerErr := make(chan error, 1)
	go func() {
		stream := newAuditStream(t, ctx)
		req := &servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{
			PageSize: 2,
			Filter:   singleFieldFilter(),
			Read:     &commonpb.ReadOptions{MinLogSequence: 5},
		}}
		handlerErr <- impl.ListAuditEntries(req, stream)
	}()

	// The handler must be blocked on WaitForAuditSequence (audit cursor 3 < head 9).
	select {
	case <-controllerCalled:
		t.Fatal("controller called before audit index caught up to the live audit head")
	case <-time.After(100 * time.Millisecond):
	}

	// Advance the audit index to the live head and wake waiters, as the indexer
	// would after committing a batch.
	writeReadstoreAuditProgress(t, rs, 9)
	rs.NotifyProgress()

	select {
	case <-controllerCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("controller not called after audit index caught up")
	}

	select {
	case err := <-handlerErr:
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after controller call")
	}
}

// TestListAuditEntriesFilteredDoesNotWaitOnLogSequenceInAuditSpace is the
// regression for the sequence-space mismatch: the audit head (2) sits BELOW
// min_log_sequence (10) because an intervening failed proposal advanced the log
// space without producing an audit entry above 2. A naive implementation that
// waited for audit_progress >= min_log_sequence would block forever (the audit
// index can never exceed the audit head of 2). The correct implementation waits
// only for audit_progress >= live audit head, so it must proceed.
func TestListAuditEntriesFilteredDoesNotWaitOnLogSequenceInAuditSpace(t *testing.T) {
	t.Parallel()

	impl, mockCtrl, mainStore, rs := newAuditConsistencyHarness(t)

	writeReadstoreLogProgress(t, rs, 10)
	// Live audit head is only 2 — far below min_log_sequence.
	writeMainAuditEntry(t, mainStore, 2)
	// Audit index already at the head.
	writeReadstoreAuditProgress(t, rs, 2)

	mockCtrl.EXPECT().
		ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Not(gomock.Nil()), gomock.Any()).
		Return(cursor.NewSliceCursor([]*auditpb.AuditEntry(nil)), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream := newAuditStream(t, ctx)
	req := &servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{
		PageSize: 2,
		Filter:   singleFieldFilter(),
		Read:     &commonpb.ReadOptions{MinLogSequence: 10},
	}}

	// Must return promptly: audit_progress(2) >= audit head(2), regardless of
	// min_log_sequence(10). A wait on audit_progress >= 10 would hit the ctx
	// deadline instead.
	require.NoError(t, impl.ListAuditEntries(req, stream))
}

// TestListAuditEntriesUnfilteredDoesNotWaitOnAuditProgress verifies an
// unfiltered live read gates only on the log index and never blocks on audit
// progress: the audit index lags (cursor 0, head 9) but the read still returns.
func TestListAuditEntriesUnfilteredDoesNotWaitOnAuditProgress(t *testing.T) {
	t.Parallel()

	impl, mockCtrl, mainStore, rs := newAuditConsistencyHarness(t)

	writeReadstoreLogProgress(t, rs, 5)
	writeMainAuditEntry(t, mainStore, 9)
	// Audit index deliberately left at 0 — an unfiltered read must not care.

	mockCtrl.EXPECT().
		ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Nil(), gomock.Any()).
		Return(cursor.NewSliceCursor([]*auditpb.AuditEntry(nil)), nil)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	stream := newAuditStream(t, ctx)
	req := &servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{
		PageSize: 2,
		// no Filter → unfiltered fast path
		Read: &commonpb.ReadOptions{MinLogSequence: 5},
	}}

	require.NoError(t, impl.ListAuditEntries(req, stream))
}

// TestListAuditEntriesFilteredContextCancelWhileWaiting verifies a filtered read
// blocked on audit progress returns promptly with a context error when the
// request context is cancelled, rather than hanging.
func TestListAuditEntriesFilteredContextCancelWhileWaiting(t *testing.T) {
	t.Parallel()

	impl, mockCtrl, mainStore, rs := newAuditConsistencyHarness(t)

	writeReadstoreLogProgress(t, rs, 5)
	writeMainAuditEntry(t, mainStore, 100)
	writeReadstoreAuditProgress(t, rs, 1) // will never catch up in this test

	// Controller must never be reached.
	mockCtrl.EXPECT().
		ListAuditEntries(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Times(0)

	ctx, cancel := context.WithCancel(context.Background())

	handlerErr := make(chan error, 1)
	go func() {
		stream := newAuditStream(t, ctx)
		req := &servicepb.ListAuditEntriesRequest{Options: &commonpb.ListOptions{
			PageSize: 2,
			Filter:   singleFieldFilter(),
			Read:     &commonpb.ReadOptions{MinLogSequence: 5},
		}}
		handlerErr <- impl.ListAuditEntries(req, stream)
	}()

	select {
	case err := <-handlerErr:
		t.Fatalf("handler returned before cancel: %v", err)
	case <-time.After(100 * time.Millisecond):
	}

	cancel()

	select {
	case err := <-handlerErr:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(5 * time.Second):
		t.Fatal("handler did not return after context cancel")
	}
}
