package events_test

import (
	"testing"

	libtime "github.com/formancehq/go-libs/v3/time"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/service/events"
	"github.com/formancehq/ledger-v3-poc/internal/service/state"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
	"github.com/stretchr/testify/require"
)

// setSinkCursorViaBatch writes a per-sink events cursor via a batch (as the FSM does).
func setSinkCursorViaBatch(t *testing.T, s *dal.Store, sinkName string, sequence uint64) {
	t.Helper()
	batch := s.NewBatch()
	require.NoError(t, state.SetSinkCursor(batch, sinkName, sequence))
	require.NoError(t, batch.Commit())
}

func TestSinkCursor(t *testing.T) {
	t.Parallel()

	t.Run("InitialCursorIsZero", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		cursor, err := events.ReadSinkCursor(s, "my-sink")
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor)
	})

	t.Run("SetAndGetCursor", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "my-sink", 42)

		cursor, err := events.ReadSinkCursor(s, "my-sink")
		require.NoError(t, err)
		require.Equal(t, uint64(42), cursor)
	})

	t.Run("CursorOverwrite", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "my-sink", 10)
		setSinkCursorViaBatch(t, s, "my-sink", 20)

		cursor, err := events.ReadSinkCursor(s, "my-sink")
		require.NoError(t, err)
		require.Equal(t, uint64(20), cursor)
	})

	t.Run("CursorPersistsAcrossReads", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "my-sink", 99)

		// Read multiple times to ensure consistency
		for range 3 {
			cursor, err := events.ReadSinkCursor(s, "my-sink")
			require.NoError(t, err)
			require.Equal(t, uint64(99), cursor)
		}
	})

	t.Run("IndependentPerSink", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "sink-a", 10)
		setSinkCursorViaBatch(t, s, "sink-b", 20)

		cursorA, err := events.ReadSinkCursor(s, "sink-a")
		require.NoError(t, err)
		require.Equal(t, uint64(10), cursorA)

		cursorB, err := events.ReadSinkCursor(s, "sink-b")
		require.NoError(t, err)
		require.Equal(t, uint64(20), cursorB)
	})
}

func TestSinkStatus(t *testing.T) {
	t.Parallel()

	t.Run("NilWhenNotSet", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		statuses, err := events.ReadAllSinkStatuses(s)
		require.NoError(t, err)
		require.Empty(t, statuses)
	})

	t.Run("SaveAndLoad", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, state.SetSinkStatus(batch, &commonpb.SinkStatus{
			SinkName: "nats-1",
			Cursor:   42,
			Error: &commonpb.SinkError{
				Message:    "connection refused",
				OccurredAt: commonpb.NewTimestamp(libtime.Now()),
			},
		}))
		require.NoError(t, batch.Commit())

		statuses, err := events.ReadAllSinkStatuses(s)
		require.NoError(t, err)
		require.Len(t, statuses, 1)
		require.Equal(t, "nats-1", statuses[0].SinkName)
		require.Equal(t, uint64(42), statuses[0].Cursor)
		require.Equal(t, "connection refused", statuses[0].Error.Message)
	})

	t.Run("ClearStatus", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		// Set a status
		batch := s.NewBatch()
		require.NoError(t, state.SetSinkStatus(batch, &commonpb.SinkStatus{
			SinkName: "nats-1",
			Cursor:   10,
		}))
		require.NoError(t, batch.Commit())

		// Clear it
		batch = s.NewBatch()
		require.NoError(t, state.ClearSinkStatus(batch, "nats-1"))
		require.NoError(t, batch.Commit())

		statuses, err := events.ReadAllSinkStatuses(s)
		require.NoError(t, err)
		require.Empty(t, statuses)
	})

	t.Run("MultipleSinks", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, state.SetSinkStatus(batch, &commonpb.SinkStatus{
			SinkName: "nats-1",
			Cursor:   10,
		}))
		require.NoError(t, state.SetSinkStatus(batch, &commonpb.SinkStatus{
			SinkName: "nats-2",
			Cursor:   20,
		}))
		require.NoError(t, batch.Commit())

		statuses, err := events.ReadAllSinkStatuses(s)
		require.NoError(t, err)
		require.Len(t, statuses, 2)
	})
}

func TestSinkConfig(t *testing.T) {
	t.Parallel()

	t.Run("EmptyWhenNotSet", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		configs, err := events.ReadAllSinkConfigs(s)
		require.NoError(t, err)
		require.Empty(t, configs)
	})

	t.Run("SaveAndLoadSingle", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, state.SaveSinkConfig(batch, &commonpb.SinkConfig{
			Name: "primary-nats",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{
					Url:   "nats://localhost:4222",
					Topic: "ledger.events",
				},
			},
			Format:       "json",
			BatchSize:    32,
			BatchDelayMs: 50,
		}))
		require.NoError(t, batch.Commit())

		cfg, err := events.ReadSinkConfig(s, "primary-nats")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "primary-nats", cfg.Name)
		require.Equal(t, "json", cfg.Format)
		require.Equal(t, int32(32), cfg.BatchSize)
		require.Equal(t, int64(50), cfg.BatchDelayMs)
		natsCfg := cfg.GetNats()
		require.NotNil(t, natsCfg)
		require.Equal(t, "nats://localhost:4222", natsCfg.Url)
		require.Equal(t, "ledger.events", natsCfg.Topic)
	})

	t.Run("LoadAllSinkConfigs", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, state.SaveSinkConfig(batch, &commonpb.SinkConfig{
			Name:   "sink-a",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://a:4222"},
			},
		}))
		require.NoError(t, state.SaveSinkConfig(batch, &commonpb.SinkConfig{
			Name:   "sink-b",
			Format: "protobuf",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://b:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		configs, err := events.ReadAllSinkConfigs(s)
		require.NoError(t, err)
		require.Len(t, configs, 2)
	})

	t.Run("DeleteSinkConfig", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		// Save two sinks
		batch := s.NewBatch()
		require.NoError(t, state.SaveSinkConfig(batch, &commonpb.SinkConfig{
			Name:   "sink-a",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://a:4222"},
			},
		}))
		require.NoError(t, state.SaveSinkConfig(batch, &commonpb.SinkConfig{
			Name:   "sink-b",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://b:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		// Delete one
		batch = s.NewBatch()
		require.NoError(t, state.DeleteSinkConfig(batch, "sink-a"))
		require.NoError(t, batch.Commit())

		configs, err := events.ReadAllSinkConfigs(s)
		require.NoError(t, err)
		require.Len(t, configs, 1)
		require.Equal(t, "sink-b", configs[0].Name)

		// Verify the deleted one returns nil
		cfg, err := events.ReadSinkConfig(s, "sink-a")
		require.NoError(t, err)
		require.Nil(t, cfg)
	})

	t.Run("UpsertSinkConfig", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		// Save initial config
		batch := s.NewBatch()
		require.NoError(t, state.SaveSinkConfig(batch, &commonpb.SinkConfig{
			Name:   "my-sink",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://old:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		// Overwrite with new URL
		batch = s.NewBatch()
		require.NoError(t, state.SaveSinkConfig(batch, &commonpb.SinkConfig{
			Name:   "my-sink",
			Format: "protobuf",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://new:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		cfg, err := events.ReadSinkConfig(s, "my-sink")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "protobuf", cfg.Format)
		require.Equal(t, "nats://new:4222", cfg.GetNats().Url)

		// Should still be only one config
		configs, err := events.ReadAllSinkConfigs(s)
		require.NoError(t, err)
		require.Len(t, configs, 1)
	})
}
