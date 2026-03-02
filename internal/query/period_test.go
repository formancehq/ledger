package query_test

import (
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/metric/noop"
)

func TestReadPeriods(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()
	logger := logging.FromContext(ctx)
	meter := noop.NewMeterProvider().Meter("test")

	t.Run("EmptyStore", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		periods, err := query.ReadAllPeriods(s)
		require.NoError(t, err)
		require.Nil(t, periods)

		nextID, err := query.ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(1), nextID)
	})

	t.Run("StoreSinglePeriod", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		batch := s.NewBatch()
		require.NoError(t, state.StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, state.StoreNextPeriodID(batch, 2))
		require.NoError(t, batch.Commit())

		periods, err := query.ReadAllPeriods(s)
		require.NoError(t, err)
		require.Len(t, periods, 1)
		require.Equal(t, uint64(1), periods[0].Id)
		require.Equal(t, uint64(1000), periods[0].Start.Data)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, periods[0].Status)

		nextID, err := query.ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(2), nextID)
	})

	t.Run("StoreMultiplePeriodsOrderedByID", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Insert periods out of order
		batch := s.NewBatch()
		require.NoError(t, state.StorePeriod(batch, &commonpb.Period{
			Id:     3,
			Start:  &commonpb.Timestamp{Data: 3000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, state.StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			End:    &commonpb.Timestamp{Data: 2000},
			Status: commonpb.PeriodStatus_PERIOD_CLOSED,
		}))
		require.NoError(t, state.StorePeriod(batch, &commonpb.Period{
			Id:            2,
			Start:         &commonpb.Timestamp{Data: 2000},
			End:           &commonpb.Timestamp{Data: 3000},
			Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
			CloseSequence: 10,
			SealingHash:   []byte("hash-2"),
		}))
		require.NoError(t, state.StoreNextPeriodID(batch, 4))
		require.NoError(t, batch.Commit())

		// Verify periods are returned ordered by ID
		periods, err := query.ReadAllPeriods(s)
		require.NoError(t, err)
		require.Len(t, periods, 3)
		require.Equal(t, uint64(1), periods[0].Id)
		require.Equal(t, uint64(2), periods[1].Id)
		require.Equal(t, uint64(3), periods[2].Id)

		// Verify fields
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, periods[0].Status)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, periods[1].Status)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_OPEN, periods[2].Status)
		require.Equal(t, uint64(10), periods[1].CloseSequence)
		require.Equal(t, []byte("hash-2"), periods[1].SealingHash)

		nextID, err := query.ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(4), nextID)
	})

	t.Run("UpdateExistingPeriod", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Store initial period
		batch := s.NewBatch()
		require.NoError(t, state.StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, state.StoreNextPeriodID(batch, 2))
		require.NoError(t, batch.Commit())

		// Update the same period (close it)
		batch = s.NewBatch()
		require.NoError(t, state.StorePeriod(batch, &commonpb.Period{
			Id:            1,
			Start:         &commonpb.Timestamp{Data: 1000},
			End:           &commonpb.Timestamp{Data: 2000},
			Status:        commonpb.PeriodStatus_PERIOD_CLOSED,
			CloseSequence: 5,
			SealingHash:   []byte("sealed"),
		}))
		require.NoError(t, batch.Commit())

		periods, err := query.ReadAllPeriods(s)
		require.NoError(t, err)
		require.Len(t, periods, 1)
		require.Equal(t, commonpb.PeriodStatus_PERIOD_CLOSED, periods[0].Status)
		require.Equal(t, uint64(5), periods[0].CloseSequence)
		require.Equal(t, []byte("sealed"), periods[0].SealingHash)
	})

	t.Run("PersistAcrossReopen", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()

		// Store periods and close
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)

		batch := s.NewBatch()
		require.NoError(t, state.StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			End:    &commonpb.Timestamp{Data: 2000},
			Status: commonpb.PeriodStatus_PERIOD_CLOSED,
		}))
		require.NoError(t, state.StorePeriod(batch, &commonpb.Period{
			Id:     2,
			Start:  &commonpb.Timestamp{Data: 2000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, state.StoreNextPeriodID(batch, 3))
		require.NoError(t, batch.Commit())

		// Create snapshot so data survives reopen (writes use NoSync)
		_, err = s.CreateSnapshot()
		require.NoError(t, err)
		require.NoError(t, s.Close())

		// Reopen and verify
		s2, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s2.Close() })

		periods, err := query.ReadAllPeriods(s2)
		require.NoError(t, err)
		require.Len(t, periods, 2)
		require.Equal(t, uint64(1), periods[0].Id)
		require.Equal(t, uint64(2), periods[1].Id)

		nextID, err := query.ReadNextPeriodID(s2)
		require.NoError(t, err)
		require.Equal(t, uint64(3), nextID)
	})

	t.Run("NextPeriodIDUpdate", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		// Set to 5
		batch := s.NewBatch()
		require.NoError(t, state.StoreNextPeriodID(batch, 5))
		require.NoError(t, batch.Commit())

		nextID, err := query.ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(5), nextID)

		// Update to 10
		batch = s.NewBatch()
		require.NoError(t, state.StoreNextPeriodID(batch, 10))
		require.NoError(t, batch.Commit())

		nextID, err = query.ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(10), nextID)
	})

	t.Run("AtomicBatchWithPeriodsAndLogs", func(t *testing.T) {
		t.Parallel()
		tmpDir := t.TempDir()
		s, err := dal.NewStore(tmpDir, logger, meter, dal.DefaultConfig())
		require.NoError(t, err)
		t.Cleanup(func() { _ = s.Close() })

		registerLedger(t, s, "test-ledger")

		// Store periods, nextPeriodID, and logs in the same batch
		batch := s.NewBatch()
		require.NoError(t, state.StorePeriod(batch, &commonpb.Period{
			Id:     1,
			Start:  &commonpb.Timestamp{Data: 1000},
			Status: commonpb.PeriodStatus_PERIOD_OPEN,
		}))
		require.NoError(t, state.StoreNextPeriodID(batch, 2))
		require.NoError(t, state.SetAppliedIndex(batch, 42))
		require.NoError(t, batch.Commit())

		// Verify all data was written atomically
		periods, err := query.ReadAllPeriods(s)
		require.NoError(t, err)
		require.Len(t, periods, 1)

		nextID, err := query.ReadNextPeriodID(s)
		require.NoError(t, err)
		require.Equal(t, uint64(2), nextID)

		lastIndex, err := query.ReadLastAppliedIndex(s)
		require.NoError(t, err)
		require.Equal(t, uint64(42), lastIndex)
	})
}
