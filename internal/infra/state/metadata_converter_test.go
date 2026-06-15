package state

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger/v3/internal/infra/attributes"
	"github.com/formancehq/ledger/v3/internal/pkg/worker"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

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

// registerLedgerWithSchema registers a ledger with a metadata schema.
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
	proposer Proposer,
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
	proposer := NewMockProposer(ctrl)

	mc, _ := newTestConverter(t, store, proposer, func() bool { return true }, 10, 2)
	mc.Start()
	// Stop is called by cleanup — this just verifies no deadlock or panic.
}

func TestMetadataConverterFieldNoLongerConverting(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockProposer(ctrl)
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
	proposer := NewMockProposer(ctrl)
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
	proposer := NewMockProposer(ctrl)

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

	proposer.EXPECT().ProposeProposal(gomock.Any()).DoAndReturn(func(cmd *raftcmdpb.Proposal) error {
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

	assert.NotEmpty(t, lastProposal.GetMetadataConversionsComplete(), "expected MetadataConversionsComplete in proposal")
}

func TestMetadataConverterPoolConcurrency(t *testing.T) {
	t.Parallel()

	ctrl := gomock.NewController(t)
	store := newConverterTestStore(t)
	proposer := NewMockProposer(ctrl)

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

	proposer.EXPECT().ProposeProposal(gomock.Any()).DoAndReturn(func(cmd *raftcmdpb.Proposal) error {
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
	proposer := NewMockProposer(ctrl)
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
	proposer := NewMockProposer(ctrl)
	// Allow any calls (fields are COMPLETE so no proposals, but allow for safety).
	proposer.EXPECT().ProposeProposal(gomock.Any()).Return(nil).AnyTimes()

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
