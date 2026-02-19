package state

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/raftcmdpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/storage/data"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func createSealerTestStore(t *testing.T) *data.Store {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	s, err := data.NewStore(t.TempDir(), logger, meter, data.DefaultConfig())
	require.NoError(t, err)
	t.Cleanup(func() { _ = s.Close() })

	return s
}

// testSealerResult captures the proposed sealing hash instead of sending it over Raft.
type testSealerResult struct {
	periodID    uint64
	sealingHash []byte
}

func newTestSealer(t *testing.T, store *data.Store) (*Sealer, *testSealerResult) {
	t.Helper()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)

	result := &testSealerResult{}
	sealer := NewSealer(logger, store, make(chan SealRequest, 1), func(periodID uint64, sealingHash []byte) {
		result.periodID = periodID
		result.sealingHash = sealingHash
	}, func() bool { return true }, func(uint64) bool { return true })

	return sealer, result
}

// createSealCheckpoint creates a seal checkpoint from the store and returns its path.
// The checkpoint is automatically cleaned up when the test finishes.
func createSealCheckpoint(t *testing.T, store *data.Store) string {
	t.Helper()

	checkpointPath, err := store.CreateTemporaryCheckpoint("seal")
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.RemoveTemporaryCheckpoint("seal") })

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
		require.NoError(t, attrs.Volume.SetBase(batch, 1, []byte("l\x00a\x00USD"), &raftcmdpb.VolumePair{
			InputKnown: commonpb.NewUint256FromUint64(uint64(500)),
		}))
		require.NoError(t, attrs.Metadata.SetBase(batch, 1, []byte("l\x00a\x00key"), &commonpb.MetadataValue{Value: "val"}))
		require.NoError(t, batch.Commit())

		checkpointPath := createSealCheckpoint(t, store)

		sealer, result := newTestSealer(t, store)
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
	require.NoError(t, attrs.Volume.SetBase(batch, 1, []byte("l\x00a\x00USD"), &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(uint64(100)),
	}))
	require.NoError(t, batch.Commit())

	// Create checkpoint BEFORE writing more data
	checkpointPath := createSealCheckpoint(t, store)

	sealer, result := newTestSealer(t, store)
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
	require.NoError(t, attrs.Volume.SetBase(batch2, 20, []byte("l\x00b\x00EUR"), &raftcmdpb.VolumePair{
		InputKnown: commonpb.NewUint256FromUint64(uint64(999)),
	}))
	require.NoError(t, batch2.Commit())

	// Create a NEW checkpoint that includes the index-20 data
	checkpointPath2 := createSealCheckpoint(t, store)

	sealer2, result2 := newTestSealer(t, store)
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

	checkpointPath := createSealCheckpoint(t, store)

	sealer, result := newTestSealer(t, store)

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
	realPath := createSealCheckpoint(t, store)
	hiddenPath := realPath + ".hidden"
	require.NoError(t, os.Rename(realPath, hiddenPath))

	var (
		proposeCalled atomic.Int32
		result        testSealerResult
	)

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	sealer := NewSealer(logger, store, make(chan SealRequest, 1), func(periodID uint64, sealingHash []byte) {
		result.periodID = periodID
		result.sealingHash = sealingHash
		proposeCalled.Add(1)
	}, func() bool { return true }, func(uint64) bool { return true })

	req := SealRequest{
		PeriodID:       7,
		CloseSequence:  50,
		LastLogHash:    []byte("test-hash"),
		CheckpointPath: realPath,
	}

	// Restore the checkpoint after a short delay so the retry succeeds
	go func() {
		time.Sleep(50 * time.Millisecond)
		_ = os.Rename(hiddenPath, realPath)
	}()

	// sealWithRetry blocks until success
	sealer.sealWithRetry(req)

	require.Equal(t, int32(1), proposeCalled.Load(), "propose should be called exactly once after retry succeeds")
	require.Equal(t, uint64(7), result.periodID)
	require.NotEmpty(t, result.sealingHash)
}
