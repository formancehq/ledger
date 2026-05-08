package events_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	libtime "github.com/formancehq/go-libs/v5/pkg/types/time"

	"github.com/formancehq/ledger-v3-poc/internal/domain"
	"github.com/formancehq/ledger-v3-poc/internal/infra/attributes"
	"github.com/formancehq/ledger-v3-poc/internal/infra/state"
	"github.com/formancehq/ledger-v3-poc/internal/proto/commonpb"
	"github.com/formancehq/ledger-v3-poc/internal/query"
	"github.com/formancehq/ledger-v3-poc/internal/storage/dal"
)

func readSinkConfig(attr *attributes.Attribute[*commonpb.SinkConfig], reader dal.PebbleReader, name string) (*commonpb.SinkConfig, error) {
	return attr.Get(reader, domain.SinkConfigKey{Name: name}.Bytes())
}

func saveSinkConfigBatch(batch *dal.Batch, cfg *commonpb.SinkConfig) error {
	attr := attributes.NewSinkConfigAttribute()
	_, err := attr.Set(batch, domain.SinkConfigKey{Name: cfg.GetName()}.Bytes(), cfg)

	return err
}

func deleteSinkConfigBatch(batch *dal.Batch, name string) error {
	attr := attributes.NewSinkConfigAttribute()

	return attr.Delete(batch, domain.SinkConfigKey{Name: name}.Bytes())
}

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

		cursor, err := query.ReadSinkCursor(s, "my-sink")
		require.NoError(t, err)
		require.Equal(t, uint64(0), cursor)
	})

	t.Run("SetAndGetCursor", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "my-sink", 42)

		cursor, err := query.ReadSinkCursor(s, "my-sink")
		require.NoError(t, err)
		require.Equal(t, uint64(42), cursor)
	})

	t.Run("CursorOverwrite", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "my-sink", 10)
		setSinkCursorViaBatch(t, s, "my-sink", 20)

		cursor, err := query.ReadSinkCursor(s, "my-sink")
		require.NoError(t, err)
		require.Equal(t, uint64(20), cursor)
	})

	t.Run("CursorPersistsAcrossReads", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "my-sink", 99)

		// Read multiple times to ensure consistency
		for range 3 {
			cursor, err := query.ReadSinkCursor(s, "my-sink")
			require.NoError(t, err)
			require.Equal(t, uint64(99), cursor)
		}
	})

	t.Run("IndependentPerSink", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		setSinkCursorViaBatch(t, s, "sink-a", 10)
		setSinkCursorViaBatch(t, s, "sink-b", 20)

		cursorA, err := query.ReadSinkCursor(s, "sink-a")
		require.NoError(t, err)
		require.Equal(t, uint64(10), cursorA)

		cursorB, err := query.ReadSinkCursor(s, "sink-b")
		require.NoError(t, err)
		require.Equal(t, uint64(20), cursorB)
	})
}

func TestSinkStatus(t *testing.T) {
	t.Parallel()

	t.Run("NilWhenNotSet", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		statuses, err := query.ReadAllSinkStatuses(s)
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

		statuses, err := query.ReadAllSinkStatuses(s)
		require.NoError(t, err)
		require.Len(t, statuses, 1)
		require.Equal(t, "nats-1", statuses[0].GetSinkName())
		require.Equal(t, uint64(42), statuses[0].GetCursor())
		require.Equal(t, "connection refused", statuses[0].GetError().GetMessage())
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

		statuses, err := query.ReadAllSinkStatuses(s)
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

		statuses, err := query.ReadAllSinkStatuses(s)
		require.NoError(t, err)
		require.Len(t, statuses, 2)
	})
}

func TestSinkConfig(t *testing.T) {
	t.Parallel()

	t.Run("EmptyWhenNotSet", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		configs, err := query.ReadAllSinkConfigs(attributes.NewSinkConfigAttribute(), s)
		require.NoError(t, err)
		require.Empty(t, configs)
	})

	t.Run("SaveAndLoadSingle", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, saveSinkConfigBatch(batch, &commonpb.SinkConfig{
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

		cfg, err := readSinkConfig(attributes.NewSinkConfigAttribute(), s, "primary-nats")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "primary-nats", cfg.GetName())
		require.Equal(t, "json", cfg.GetFormat())
		require.Equal(t, int32(32), cfg.GetBatchSize())
		require.Equal(t, int64(50), cfg.GetBatchDelayMs())
		natsCfg := cfg.GetNats()
		require.NotNil(t, natsCfg)
		require.Equal(t, "nats://localhost:4222", natsCfg.GetUrl())
		require.Equal(t, "ledger.events", natsCfg.GetTopic())
	})

	t.Run("LoadAllSinkConfigs", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		batch := s.NewBatch()
		require.NoError(t, saveSinkConfigBatch(batch, &commonpb.SinkConfig{
			Name:   "sink-a",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://a:4222"},
			},
		}))
		require.NoError(t, saveSinkConfigBatch(batch, &commonpb.SinkConfig{
			Name:   "sink-b",
			Format: "protobuf",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://b:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		configs, err := query.ReadAllSinkConfigs(attributes.NewSinkConfigAttribute(), s)
		require.NoError(t, err)
		require.Len(t, configs, 2)
	})

	t.Run("DeleteSinkConfig", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		// Save two sinks
		batch := s.NewBatch()
		require.NoError(t, saveSinkConfigBatch(batch, &commonpb.SinkConfig{
			Name:   "sink-a",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://a:4222"},
			},
		}))
		require.NoError(t, saveSinkConfigBatch(batch, &commonpb.SinkConfig{
			Name:   "sink-b",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://b:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		// Delete one
		batch = s.NewBatch()
		require.NoError(t, deleteSinkConfigBatch(batch, "sink-a"))
		require.NoError(t, batch.Commit())

		configs, err := query.ReadAllSinkConfigs(attributes.NewSinkConfigAttribute(), s)
		require.NoError(t, err)
		require.Len(t, configs, 1)
		require.Equal(t, "sink-b", configs[0].GetName())

		// Verify the deleted one returns nil
		cfg, err := readSinkConfig(attributes.NewSinkConfigAttribute(), s, "sink-a")
		require.NoError(t, err)
		require.Nil(t, cfg)
	})

	t.Run("UpsertSinkConfig", func(t *testing.T) {
		t.Parallel()
		s := newTestStore(t)

		// Save initial config
		batch := s.NewBatch()
		require.NoError(t, saveSinkConfigBatch(batch, &commonpb.SinkConfig{
			Name:   "my-sink",
			Format: "json",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://old:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		// Overwrite with new URL
		batch = s.NewBatch()
		require.NoError(t, saveSinkConfigBatch(batch, &commonpb.SinkConfig{
			Name:   "my-sink",
			Format: "protobuf",
			Type: &commonpb.SinkConfig_Nats{
				Nats: &commonpb.NatsSinkConfig{Url: "nats://new:4222"},
			},
		}))
		require.NoError(t, batch.Commit())

		cfg, err := readSinkConfig(attributes.NewSinkConfigAttribute(), s, "my-sink")
		require.NoError(t, err)
		require.NotNil(t, cfg)
		require.Equal(t, "protobuf", cfg.GetFormat())
		require.Equal(t, "nats://new:4222", cfg.GetNats().GetUrl())

		// Should still be only one config
		configs, err := query.ReadAllSinkConfigs(attributes.NewSinkConfigAttribute(), s)
		require.NoError(t, err)
		require.Len(t, configs, 1)
	})
}
