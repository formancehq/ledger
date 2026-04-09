package state

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func createSealerTestStore(t *testing.T) *dal.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := dal.NewStore(t.TempDir(), logger, meter, dal.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// testSealerResult captures the proposed sealing hash instead of sending it over Raft.
type testSealerResult struct {
	periodID    uint64
	sealingHash []byte
	stateHash   []byte
}

// fixedPeriodState returns a fixed closing period for testing.
type fixedPeriodState struct {
	period *commonpb.Period
}

func (f *fixedPeriodState) ClosingPeriods() []*commonpb.Period {
	if f.period == nil {
		return nil
	}

	return []*commonpb.Period{f.period}
}

func (f *fixedPeriodState) ClosingPeriodByID(id uint64) (*commonpb.Period, bool) {
	if f.period != nil && f.period.GetId() == id {
		return f.period, true
	}

	return nil, false
}

// multiPeriodState holds multiple closing periods for testing.
type multiPeriodState struct {
	periods []*commonpb.Period
}

func (m *multiPeriodState) ClosingPeriods() []*commonpb.Period {
	return m.periods
}

func (m *multiPeriodState) ClosingPeriodByID(id uint64) (*commonpb.Period, bool) {
	for _, p := range m.periods {
		if p.GetId() == id {
			return p, true
		}
	}

	return nil, false
}

func newTestSealer(t *testing.T, store *dal.Store, closingPeriodID uint64) (*Sealer, *testSealerResult) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	result := &testSealerResult{}
	ps := &fixedPeriodState{period: &commonpb.Period{Id: closingPeriodID}}
	sealer := NewSealer(logger, store, attributes.New(), make(chan SealRequest, 1), func(periodID uint64, sealingHash, stateHash []byte) {
		result.periodID = periodID
		result.sealingHash = sealingHash
		result.stateHash = stateHash
	}, func() bool { return true }, ps)

	return sealer, result
}

// createSealCheckpoint creates a seal checkpoint from the store and returns its path.
// The checkpoint is automatically cleaned up when the test finishes.
func createSealCheckpoint(t *testing.T, store *dal.Store, periodID uint64) string {
	t.Helper()

	name := SealCheckpointName(periodID)
	checkpointPath, err := store.CreateTemporaryCheckpoint(name)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.RemoveTemporaryCheckpoint(name) })

	return checkpointPath
}

func TestSealerDeterministic(t *testing.T) {
	t.Parallel()

	// Run the same sealing twice on identical state
	var hashes [2][]byte

	for i := range hashes {
		store := createSealerTestStore(t)
		attrs := attributes.New()

		batch := store.NewBatch()
		require.NoError(t, attrs.Volume.Set(batch, []byte("l\x00a\x00USD"), &raftcmdpb.VolumePair{
			Input: commonpb.NewUint256FromUint64(uint64(500)),
		}))
		require.NoError(t, attrs.Metadata.Set(batch, []byte("l\x00a\x00key"), commonpb.NewStringValue("val")))
		require.NoError(t, batch.Commit())

		checkpointPath := createSealCheckpoint(t, store, 42)

		sealer, result := newTestSealer(t, store, 42)
		err := sealer.seal(SealRequest{
			PeriodID:       42,
			CloseSequence:  100,
			LastLogHash:    []byte("chain-hash"),
			CheckpointPath: checkpointPath,
		})
		require.NoError(t, err)

		hashes[i] = result.sealingHash
	}

	require.Equal(t, hashes[0], hashes[1], "same state should produce same sealing hash")
}

func TestSealerCheckpointIsolation(t *testing.T) {
	t.Parallel()

	store := createSealerTestStore(t)
	attrs := attributes.New()

	// Write data at index 1
	batch := store.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch, []byte("l\x00a\x00USD"), &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(uint64(100)),
	}))
	require.NoError(t, batch.Commit())

	// Create checkpoint BEFORE writing more data
	checkpointPath := createSealCheckpoint(t, store, 42)

	sealer, result := newTestSealer(t, store, 1)
	err := sealer.seal(SealRequest{
		PeriodID:       1,
		CloseSequence:  10,
		LastLogHash:    nil,
		CheckpointPath: checkpointPath,
	})
	require.NoError(t, err)

	hashBefore := result.sealingHash

	// Write additional data at index 20 (after the checkpoint was taken)
	batch2 := store.NewBatch()
	require.NoError(t, attrs.Volume.Set(batch2, []byte("l\x00b\x00EUR"), &raftcmdpb.VolumePair{
		Input: commonpb.NewUint256FromUint64(uint64(999)),
	}))
	require.NoError(t, batch2.Commit())

	// Create a NEW checkpoint that includes the index-20 data
	checkpointPath2 := createSealCheckpoint(t, store, 42)

	sealer2, result2 := newTestSealer(t, store, 1)
	err = sealer2.seal(SealRequest{
		PeriodID:       1,
		CloseSequence:  10,
		LastLogHash:    nil,
		CheckpointPath: checkpointPath2,
	})
	require.NoError(t, err)

	// The hashes should DIFFER because the second checkpoint includes
	// additional data that was not in the first checkpoint.
	// This proves that the checkpoint captures the exact state — no filtering needed.
	require.NotEqual(t, hashBefore, result2.sealingHash,
		"different checkpoint contents should produce different hashes")
}

func TestSealerEmptyStore(t *testing.T) {
	t.Parallel()

	store := createSealerTestStore(t)

	checkpointPath := createSealCheckpoint(t, store, 42)

	sealer, result := newTestSealer(t, store, 1)

	err := sealer.seal(SealRequest{
		PeriodID:       1,
		CloseSequence:  10,
		LastLogHash:    nil,
		CheckpointPath: checkpointPath,
	})
	require.NoError(t, err)
	require.NotEmpty(t, result.sealingHash, "should produce a hash even with no attributes")
}

func TestSealerRetryOnFailure(t *testing.T) {
	t.Parallel()

	store := createSealerTestStore(t)

	// Create a real checkpoint, then hide it so the first attempt fails
	realPath := createSealCheckpoint(t, store, 7)
	hiddenPath := realPath + ".hidden"
	require.NoError(t, os.Rename(realPath, hiddenPath))

	var proposeCalled atomic.Int32

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	sealRequestCh := make(chan SealRequest, 1)
	ps := &fixedPeriodState{period: &commonpb.Period{Id: 7}}
	sealer := NewSealer(logger, store, attributes.New(), sealRequestCh, func(periodID uint64, sealingHash, stateHash []byte) {
		proposeCalled.Add(1)
	}, func() bool { return true }, ps)

	sealer.Start()
	t.Cleanup(sealer.Stop)

	// Restore the checkpoint so the retry succeeds
	go func() {
		_ = os.Rename(hiddenPath, realPath)
	}()

	// Send the seal request via the channel
	sealRequestCh <- SealRequest{
		PeriodID:       7,
		CloseSequence:  50,
		LastLogHash:    []byte("test-hash"),
		CheckpointPath: realPath,
	}

	require.Eventually(t, func() bool {
		return proposeCalled.Load() == 1
	}, 10*time.Second, 50*time.Millisecond, "propose should be called exactly once after retry succeeds")
}

func TestSealCheckpointName(t *testing.T) {
	t.Parallel()

	require.Equal(t, "seal-1", SealCheckpointName(1))
	require.Equal(t, "seal-42", SealCheckpointName(42))
	require.Equal(t, "seal-0", SealCheckpointName(0))

	// Different IDs produce different names
	require.NotEqual(t, SealCheckpointName(1), SealCheckpointName(2))
}

func TestSealerRecoverPendingSealMultiplePeriods(t *testing.T) {
	t.Parallel()

	store := createSealerTestStore(t)

	p1 := &commonpb.Period{Id: 5, CloseSequence: 100, LastLogHash: []byte("h1"), Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	p2 := &commonpb.Period{Id: 8, CloseSequence: 200, LastLogHash: []byte("h2"), Status: commonpb.PeriodStatus_PERIOD_CLOSING}

	// Create seal checkpoints for both periods
	_, err := store.CreateTemporaryCheckpoint(SealCheckpointName(5))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.RemoveTemporaryCheckpoint(SealCheckpointName(5)) })

	_, err = store.CreateTemporaryCheckpoint(SealCheckpointName(8))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.RemoveTemporaryCheckpoint(SealCheckpointName(8)) })

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	sealRequestCh := make(chan SealRequest, 10)
	ps := &multiPeriodState{periods: []*commonpb.Period{p1, p2}}

	sealer := NewSealer(logger, store, attributes.New(), sealRequestCh, func(uint64, []byte, []byte) {}, func() bool { return true }, ps)

	sealer.recoverPendingSeal()

	// Both periods should have been enqueued
	var received []uint64
	for len(received) < 2 {
		select {
		case req := <-sealRequestCh:
			received = append(received, req.PeriodID)
		default:
			t.Fatal("expected 2 seal requests")
		}
	}

	require.Contains(t, received, uint64(5))
	require.Contains(t, received, uint64(8))
}

func TestSealerRecoverPendingSealSkipsMissingCheckpoint(t *testing.T) {
	t.Parallel()

	store := createSealerTestStore(t)

	p1 := &commonpb.Period{Id: 5, Status: commonpb.PeriodStatus_PERIOD_CLOSING}
	p2 := &commonpb.Period{Id: 8, CloseSequence: 200, LastLogHash: []byte("h2"), Status: commonpb.PeriodStatus_PERIOD_CLOSING}

	// Only create a checkpoint for period 8, not period 5
	_, err := store.CreateTemporaryCheckpoint(SealCheckpointName(8))
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.RemoveTemporaryCheckpoint(SealCheckpointName(8)) })

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	sealRequestCh := make(chan SealRequest, 10)
	ps := &multiPeriodState{periods: []*commonpb.Period{p1, p2}}

	sealer := NewSealer(logger, store, attributes.New(), sealRequestCh, func(uint64, []byte, []byte) {}, func() bool { return true }, ps)

	sealer.recoverPendingSeal()

	// Only period 8 should be enqueued (period 5 has no checkpoint)
	select {
	case req := <-sealRequestCh:
		require.Equal(t, uint64(8), req.PeriodID)
	default:
		t.Fatal("expected 1 seal request for period 8")
	}

	// No more requests
	select {
	case req := <-sealRequestCh:
		t.Fatalf("unexpected seal request: %+v", req)
	default:
	}
}

func TestSealerSkipsAlreadySealedPeriod(t *testing.T) {
	t.Parallel()

	store := createSealerTestStore(t)
	checkpointPath := createSealCheckpoint(t, store, 42)

	// Period state says no closing periods (period was already sealed)
	ps := &fixedPeriodState{period: nil}

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	var proposeCalled bool

	sealer := NewSealer(logger, store, attributes.New(), make(chan SealRequest, 1), func(uint64, []byte, []byte) {
		proposeCalled = true
	}, func() bool { return true }, ps)

	err := sealer.seal(SealRequest{
		PeriodID:       42,
		CloseSequence:  10,
		CheckpointPath: checkpointPath,
	})
	require.NoError(t, err)
	require.False(t, proposeCalled, "should not propose when period is no longer closing")
}
