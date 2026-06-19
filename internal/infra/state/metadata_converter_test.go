package state

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

// extractBatches walks a proposal's TechnicalUpdates and returns the
// embedded MetadataConversionBatch payloads in order. Test-only convenience
// after the TechnicalUpdate envelope refactor — production code dispatches
// on the oneof in machine_technical_updates.go.
func extractBatches(cmd *raftcmdpb.Proposal) []*raftcmdpb.MetadataConversionBatch {
	var out []*raftcmdpb.MetadataConversionBatch
	for _, tu := range cmd.GetTechnicalUpdates() {
		if b, ok := tu.GetKind().(*raftcmdpb.TechnicalUpdate_MetadataBatch); ok {
			out = append(out, b.MetadataBatch)
		}
	}

	return out
}

// extractCompletions walks a proposal's TechnicalUpdates and returns the
// embedded MetadataConversionCompletion payloads in order.
func extractCompletions(cmd *raftcmdpb.Proposal) []*raftcmdpb.MetadataConversionCompletion {
	var out []*raftcmdpb.MetadataConversionCompletion
	for _, tu := range cmd.GetTechnicalUpdates() {
		if c, ok := tu.GetKind().(*raftcmdpb.TechnicalUpdate_MetadataCompletion); ok {
			out = append(out, c.MetadataCompletion)
		}
	}

	return out
}

// newConverterTestStore creates a fresh Pebble-backed dal.Store for testing.
func newConverterTestStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// testLedgerName is the ledger ID used by the test fixtures so the seeded
// metadata keys match the prefix the converter scans for.
const testLedgerName = "test-ledger"

// registerLedgerWithSchema registers a ledger with a metadata schema. The
// ledger is given a non-zero ID so test seeds can be built through
// `domain.MetadataKey{LedgerName: testLedgerName, ...}.Bytes()` — matching how
// production writes keys.
func registerLedgerWithSchema(t *testing.T, s *dal.Store, name string, schema *commonpb.MetadataSchema) {
	t.Helper()

	batch := s.OpenWriteSession()
	err := SaveLedger(batch, &commonpb.LedgerInfo{
		Name:           name,
		CreatedAt:      commonpb.NewTimestamp(libtime.Now()),
		MetadataSchema: schema,
	})
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// newTestConverter creates a MetadataConverter with sensible test defaults.
func newTestConverter(
	t *testing.T,
	store *dal.Store,
	proposer MetadataBatchProposer,
	isLeader func() bool,
	batchSize int,
	poolSize int,
) (*MetadataConverter, *worker.Channel[MetadataConvertRequest]) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	attrs := attributes.New()

	requestCh := worker.NewChannel[MetadataConvertRequest](logger, "test-convert", 100)
	mc := NewMetadataConverter(
		logger,
		store,
		attrs,
		requestCh,
		proposer,
		isLeader,
		batchSize,
		poolSize,
		func(<-chan struct{}) {},
	)
	t.Cleanup(mc.Stop)

	return mc, requestCh
}

func TestMetadataConverterStartStop(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockMetadataBatchProposer(ctrl)

	mc, _ := newTestConverter(t, store, proposer, func() bool { return true }, 10, 2)
	mc.Start()
	// Stop is called by cleanup — this just verifies no deadlock or panic.
}

func TestMetadataConverterFieldNoLongerConverting(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockMetadataBatchProposer(ctrl)
	// No EXPECT on proposer → any call will fail the test.

	// Register a ledger with the field already COMPLETE (not CONVERTING).
	registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"age": {
				Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
				Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
			},
		},
	})

	mc, requestCh := newTestConverter(t, store, proposer, func() bool { return true }, 10, 2)
	mc.Start()

	// Send a convert request for the already-complete field.
	requestCh.TrySend(MetadataConvertRequest{
		LedgerName: "test-ledger",
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "age",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	}, "test")

	// Wait for the converter to process the request. Since the field is
	// already COMPLETE, it should exit without calling ProposeProposal.
	// gomock will fail if ProposeProposal is called.
	require.Eventually(t, func() bool {
		return len(requestCh.Receive()) == 0
	}, 2*time.Second, 50*time.Millisecond)
}

func TestMetadataConverterNonLeaderWaits(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockMetadataBatchProposer(ctrl)
	// No EXPECT → non-leader must never propose.

	// Register a ledger with the field in CONVERTING state.
	registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"age": {
				Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
				Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
			},
		},
	})

	mc, requestCh := newTestConverter(t, store, proposer, func() bool { return false }, 10, 2)
	mc.Start()

	requestCh.TrySend(MetadataConvertRequest{
		LedgerName: "test-ledger",
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "age",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	}, "test")

	// Now mark the field as complete (simulating leader completing via Raft).
	registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"age": {
				Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
				Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
			},
		},
	})

	// The non-leader should eventually notice and exit.
	// gomock verifies ProposeProposal was never called.
	require.Eventually(t, func() bool {
		return len(requestCh.Receive()) == 0
	}, 5*time.Second, 100*time.Millisecond)
}

func TestMetadataConverterLeaderProposesCompletion(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockMetadataBatchProposer(ctrl)

	// Register a ledger with the field in CONVERTING state but no metadata entries.
	registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"score": {
				Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
				Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
			},
		},
	})

	// Expect exactly one ProposeProposal call: the ConversionComplete proposal.
	var (
		mu                sync.Mutex
		capturedProposals []*raftcmdpb.Proposal
	)

	proposer.EXPECT().Propose(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cmd *raftcmdpb.Proposal, _ [][]byte, _ commonpb.TargetType) error {
		mu.Lock()
		defer mu.Unlock()

		capturedProposals = append(capturedProposals, cmd)

		return nil
	}).AnyTimes()

	mc, requestCh := newTestConverter(t, store, proposer, func() bool { return true }, 10, 2)
	mc.Start()

	requestCh.TrySend(MetadataConvertRequest{
		LedgerName: "test-ledger",
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "score",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	}, "test")

	// Wait for the ConversionComplete proposal.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(capturedProposals) > 0
	}, 5*time.Second, 50*time.Millisecond)

	// The last proposal should contain a MetadataConversionsComplete entry.
	mu.Lock()
	lastProposal := capturedProposals[len(capturedProposals)-1]
	mu.Unlock()

	assert.NotEmpty(t, extractCompletions(lastProposal), "expected MetadataConversionsComplete in proposal")
}

// seedAccountMetadata writes account metadata entries through the same
// canonical-key construction the FSM write path uses
// (`domain.MetadataKey{LedgerID, Account, Key}.Bytes()`), so the converter's
// LedgerID-prefixed scan finds them.
func seedAccountMetadata(t *testing.T, s *dal.Store, _, account, key string, value *commonpb.MetadataValue) {
	t.Helper()

	attrs := attributes.New()
	canonicalKey := domain.MetadataKey{
		AccountKey: domain.AccountKey{LedgerName: testLedgerName, Account: account},
		Key:        key,
	}.Bytes()

	batch := s.OpenWriteSession()
	_, err := attrs.Metadata.Set(batch, canonicalKey, value)
	require.NoError(t, err)
	require.NoError(t, batch.Commit())
}

// TestMetadataConverterProposesOnlyKeysNeedingConversion guards that the
// converter never enqueues already-typed values into a batch — they are the
// signal that a pass has converged and Complete should be proposed instead.
// Pre-#359 cleanup, this was conflated with a separate TotalKeys counter the
// FSM used to auto-COMPLETE; that counter has been removed and the converter
// alone drives convergence by emitting Complete when a pass enqueues zero
// entries.
func TestMetadataConverterProposesOnlyKeysNeedingConversion(t *testing.T) {
	t.Parallel()

	t.Run("mixed: only untyped keys counted", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		store := newConverterTestStore(t)
		proposer := NewMockMetadataBatchProposer(ctrl)

		registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
				},
			},
		})

		// Two values need conversion (stored as strings), one already has the
		// expected int64 type and must NOT be enqueued in the batch — the
		// already-typed entry should be silently skipped at scan time.
		seedAccountMetadata(t, store, "test-ledger", "users:alice", "age", commonpb.NewStringValue("30"))
		seedAccountMetadata(t, store, "test-ledger", "users:bob", "age", commonpb.NewStringValue("41"))
		seedAccountMetadata(t, store, "test-ledger", "users:carol", "age", commonpb.NewIntValue(52))

		var (
			mu        sync.Mutex
			batches   []*raftcmdpb.MetadataConversionBatch
			completes int
		)

		proposer.EXPECT().Propose(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cmd *raftcmdpb.Proposal, _ [][]byte, _ commonpb.TargetType) error {
			mu.Lock()
			defer mu.Unlock()

			batches = append(batches, extractBatches(cmd)...)
			completes += len(extractCompletions(cmd))

			return nil
		}).AnyTimes()

		mc, requestCh := newTestConverter(t, store, proposer, func() bool { return true }, 10, 1)
		mc.Start()

		requestCh.TrySend(MetadataConvertRequest{
			LedgerName: "test-ledger",
			TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:        "age",
			Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
		}, "test")

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return len(batches) > 0
		}, 5*time.Second, 50*time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		require.GreaterOrEqual(t, len(batches), 1, "expected at least one conversion batch")
		assert.Len(t, batches[0].GetEntries(), 2,
			"only the two untyped values should be enqueued in the first batch")
		assert.Zero(t, completes,
			"completion must NOT be proposed while a pass still enqueues entries")
	})

	t.Run("all already typed: completes without a batch", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		store := newConverterTestStore(t)
		proposer := NewMockMetadataBatchProposer(ctrl)

		registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
				},
			},
		})

		// Every value already has the expected type: nothing needs a write, so
		// the converter must propose completion (else CONVERTING never clears).
		seedAccountMetadata(t, store, "test-ledger", "users:alice", "age", commonpb.NewIntValue(30))
		seedAccountMetadata(t, store, "test-ledger", "users:bob", "age", commonpb.NewIntValue(41))

		var (
			mu        sync.Mutex
			batches   int
			completes int
		)

		proposer.EXPECT().Propose(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cmd *raftcmdpb.Proposal, _ [][]byte, _ commonpb.TargetType) error {
			mu.Lock()
			defer mu.Unlock()

			batches += len(extractBatches(cmd))
			completes += len(extractCompletions(cmd))

			return nil
		}).AnyTimes()

		mc, requestCh := newTestConverter(t, store, proposer, func() bool { return true }, 10, 1)
		mc.Start()

		requestCh.TrySend(MetadataConvertRequest{
			LedgerName: "test-ledger",
			TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:        "age",
			Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
		}, "test")

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return completes > 0
		}, 5*time.Second, 50*time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		assert.Zero(t, batches, "no conversion batch expected when all values are already typed")
	})

	t.Run("all already null sentinel: completes without a batch", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		store := newConverterTestStore(t)
		proposer := NewMockMetadataBatchProposer(ctrl)

		registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"age": {
					Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
				},
			},
		})

		// Values that were already converted to the NullValue sentinel by a
		// previous conversion pass (i.e. the original value was structurally
		// inconvertible to the declared type, e.g. "abc" -> INT64). TypeMatches
		// returns false on NullValue but the converter must treat it as
		// terminal — otherwise it re-enqueues the same key forever and the
		// field stays CONVERTING indefinitely.
		seedAccountMetadata(t, store, "test-ledger", "users:alice", "age", commonpb.NewNullValue("abc"))
		seedAccountMetadata(t, store, "test-ledger", "users:bob", "age", commonpb.NewNullValue("xyz"))

		var (
			mu        sync.Mutex
			batches   int
			completes int
		)

		proposer.EXPECT().Propose(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cmd *raftcmdpb.Proposal, _ [][]byte, _ commonpb.TargetType) error {
			mu.Lock()
			defer mu.Unlock()

			batches += len(extractBatches(cmd))
			completes += len(extractCompletions(cmd))

			return nil
		}).AnyTimes()

		mc, requestCh := newTestConverter(t, store, proposer, func() bool { return true }, 10, 1)
		mc.Start()

		requestCh.TrySend(MetadataConvertRequest{
			LedgerName: "test-ledger",
			TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:        "age",
			Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
		}, "test")

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return completes > 0
		}, 5*time.Second, 50*time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		assert.Zero(t, batches, "no conversion batch expected when all values are already the NullValue sentinel")
	})

	t.Run("retyped to a more permissive type re-converts NullValue", func(t *testing.T) {
		t.Parallel()

		ctrl := gomock.NewController(t)
		store := newConverterTestStore(t)
		proposer := NewMockMetadataBatchProposer(ctrl)

		// Field now declared STRING — the original conversion to INT64
		// failed and left NullValue sentinels. Retyping to STRING must
		// recover the originals via ConvertMetadataValue's convertFromNull
		// path; the converter must NOT blanket-skip NullValue.
		registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
			AccountFields: map[string]*commonpb.MetadataFieldSchema{
				"label": {
					Type:   commonpb.MetadataType_METADATA_TYPE_STRING,
					Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
				},
			},
		})

		seedAccountMetadata(t, store, "test-ledger", "users:alice", "label", commonpb.NewNullValue("abc"))
		seedAccountMetadata(t, store, "test-ledger", "users:bob", "label", commonpb.NewNullValue("xyz"))

		var (
			mu      sync.Mutex
			batches []*raftcmdpb.MetadataConversionBatch
		)

		proposer.EXPECT().Propose(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cmd *raftcmdpb.Proposal, _ [][]byte, _ commonpb.TargetType) error {
			mu.Lock()
			defer mu.Unlock()

			batches = append(batches, extractBatches(cmd)...)

			return nil
		}).AnyTimes()

		mc, requestCh := newTestConverter(t, store, proposer, func() bool { return true }, 10, 1)
		mc.Start()

		requestCh.TrySend(MetadataConvertRequest{
			LedgerName: "test-ledger",
			TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:        "label",
			Type:       commonpb.MetadataType_METADATA_TYPE_STRING,
		}, "test")

		require.Eventually(t, func() bool {
			mu.Lock()
			defer mu.Unlock()

			return len(batches) > 0
		}, 5*time.Second, 50*time.Millisecond)

		mu.Lock()
		defer mu.Unlock()

		require.GreaterOrEqual(t, len(batches), 1, "expected at least one conversion batch")
		assert.Len(t, batches[0].GetEntries(), 2,
			"both NullValue entries must be enqueued when retyping to STRING — convertFromNull recovers the original")

		// And the converted values must be the original strings (not NullValues).
		gotOriginals := map[string]bool{}
		for _, e := range batches[0].GetEntries() {
			sv, ok := e.GetConvertedValue().GetType().(*commonpb.MetadataValue_StringValue)
			require.True(t, ok, "expected ConvertedValue to be a StringValue, got %T", e.GetConvertedValue().GetType())
			gotOriginals[sv.StringValue] = true
		}
		assert.True(t, gotOriginals["abc"], "expected original \"abc\" to be recovered as a StringValue")
		assert.True(t, gotOriginals["xyz"], "expected original \"xyz\" to be recovered as a StringValue")
	})
}

// TestMetadataConverterStopUnblocksInFlightConversion guards the shutdown
// regression (#359) where an in-flight conversion blocked teardown. The
// proposer's Propose blocks until the FSM apply resolves; at shutdown the
// Raft node stops and that apply never comes, so Propose must observe the
// converter's stop-derived context being cancelled and return. If the
// converter threaded a non-cancelable context (or the proposer ignored
// cancellation), Stop's wg.Wait would hang past the stop timeout and strand
// the app's OnStop — exactly the e2e teardown hang this protects against.
func TestMetadataConverterStopUnblocksInFlightConversion(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockMetadataBatchProposer(ctrl)

	registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"age": {
				Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
				Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
			},
		},
	})

	seedAccountMetadata(t, store, "test-ledger", "users:alice", "age", commonpb.NewStringValue("30"))

	// Propose blocks until its context is cancelled, simulating an FSM apply
	// that never resolves because the node is shutting down.
	proposeEntered := make(chan struct{})

	var once sync.Once

	proposer.EXPECT().
		Propose(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context, _ *raftcmdpb.Proposal, _ [][]byte, _ commonpb.TargetType) error {
			once.Do(func() { close(proposeEntered) })

			<-ctx.Done()

			return ctx.Err()
		}).
		AnyTimes()

	// Build the converter directly (not via newTestConverter) to avoid the
	// helper's t.Cleanup(mc.Stop): this test calls Stop explicitly, and a
	// second Stop would close the worker's stop channel twice.
	logger := logging.FromContext(logging.TestingContext())
	requestCh := worker.NewChannel[MetadataConvertRequest](logger, "test-convert", 100)
	mc := NewMetadataConverter(
		logger,
		store,
		attributes.New(),
		requestCh,
		proposer,
		func() bool { return true },
		10,
		1,
		func(<-chan struct{}) {},
	)
	mc.Start()

	requestCh.TrySend(MetadataConvertRequest{
		LedgerName: "test-ledger",
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "age",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	}, "test")

	select {
	case <-proposeEntered:
	case <-time.After(5 * time.Second):
		t.Fatal("conversion never reached Propose")
	}

	// Stop must return promptly: cancelling the conversion's context unblocks
	// the in-flight Propose so wg.Wait completes.
	stopped := make(chan struct{})
	go func() {
		mc.Stop()
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop hung on an in-flight conversion — shutdown context not honored")
	}
}

func TestMetadataConverterPoolConcurrency(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockMetadataBatchProposer(ctrl)

	// Register multiple fields in CONVERTING state.
	registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"field1": {
				Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
				Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
			},
			"field2": {
				Type:   commonpb.MetadataType_METADATA_TYPE_BOOL,
				Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
			},
			"field3": {
				Type:   commonpb.MetadataType_METADATA_TYPE_UINT64,
				Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_CONVERTING,
			},
		},
	})

	var (
		mu            sync.Mutex
		proposalCount int
	)

	proposer.EXPECT().Propose(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(_ context.Context, cmd *raftcmdpb.Proposal, _ [][]byte, _ commonpb.TargetType) error {
		mu.Lock()
		defer mu.Unlock()

		proposalCount++

		return nil
	}).AnyTimes()

	mc, requestCh := newTestConverter(t, store, proposer, func() bool { return true }, 10, 3)
	mc.Start()

	// Send 3 conversion requests concurrently.
	for _, key := range []string{"field1", "field2", "field3"} {
		mdType := commonpb.MetadataType_METADATA_TYPE_INT64

		switch key {
		case "field2":
			mdType = commonpb.MetadataType_METADATA_TYPE_BOOL
		case "field3":
			mdType = commonpb.MetadataType_METADATA_TYPE_UINT64
		}

		requestCh.TrySend(MetadataConvertRequest{
			LedgerName: "test-ledger",
			TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:        key,
			Type:       mdType,
		}, "test")
	}

	// All 3 should complete (each producing at least a ConversionComplete proposal).
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return proposalCount >= 3
	}, 5*time.Second, 50*time.Millisecond)
}

func TestMetadataConverterLedgerNotFound(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockMetadataBatchProposer(ctrl)
	// No EXPECT → proposer must never be called.

	// Don't register any ledger — the converter should handle the error gracefully.
	mc, requestCh := newTestConverter(t, store, proposer, func() bool { return true }, 10, 2)
	mc.Start()

	requestCh.TrySend(MetadataConvertRequest{
		LedgerName: "nonexistent-ledger",
		TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
		Key:        "age",
		Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
	}, "test")

	// The isFieldStillConverting check will return false (ledger not found),
	// so the converter should exit without proposing.
	require.Eventually(t, func() bool {
		return len(requestCh.Receive()) == 0
	}, 2*time.Second, 50*time.Millisecond)
}

func TestMetadataConverterQueueDrainsOnStop(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockMetadataBatchProposer(ctrl)
	// Allow any calls (fields are COMPLETE so no proposals, but allow for safety).
	proposer.EXPECT().Propose(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(nil).AnyTimes()

	// Register ledger with schema that is complete — requests will be processed
	// quickly (exit on isFieldStillConverting=false).
	registerLedgerWithSchema(t, store, "test-ledger", &commonpb.MetadataSchema{
		AccountFields: map[string]*commonpb.MetadataFieldSchema{
			"x": {
				Type:   commonpb.MetadataType_METADATA_TYPE_INT64,
				Status: commonpb.MetadataConversionStatus_METADATA_CONVERSION_COMPLETE,
			},
		},
	})

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	attrs := attributes.New()
	requestCh := worker.NewChannel[MetadataConvertRequest](logger, "test-convert", 100)

	mc := NewMetadataConverter(
		logger, store, attrs, requestCh, proposer,
		func() bool { return true }, 10, 1,
		func(<-chan struct{}) {},
	)

	// Fill the channel before starting.
	for range 10 {
		requestCh.TrySend(MetadataConvertRequest{
			LedgerName: "test-ledger",
			TargetType: commonpb.TargetType_TARGET_TYPE_ACCOUNT,
			Key:        "x",
			Type:       commonpb.MetadataType_METADATA_TYPE_INT64,
		}, "test")
	}

	mc.Start()

	// Stop should not hang — the converter processes or drops queued items.
	done := make(chan struct{})

	go func() {
		mc.Stop()
		close(done)
	}()

	select {
	case <-done:
		// OK — stopped cleanly.
	case <-time.After(5 * time.Second):
		t.Fatal("MetadataConverter.Stop() timed out — possible deadlock")
	}
}
