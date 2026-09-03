package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestWriteSetAbsorb_CoversEveryDerivedPayload pins the mapping between
// each LogPayload variant that should mutate the WriteSet's cross-order
// accumulators and the field it touches. If a new derivable LogPayload
// variant is added without a matching case in Absorb, the corresponding
// subtest here will fail (the assertion expects a non-zero
// accumulator). Payloads with no cross-order signal — Apply,
// SaveLedgerMetadata, signing/maintenance, ... — are
// covered by TestWriteSetAbsorb_NoOpForUnmappedPayloads below.
func TestWriteSetAbsorb_CoversEveryDerivedPayload(t *testing.T) {
	t.Parallel()

	t.Run("AddedEventsSink → SinkConfigs + sinkConfigChanged", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		cfg := &commonpb.SinkConfig{Name: "my-sink"}
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_AddedEventsSink{AddedEventsSink: &commonpb.AddedEventsSinkLog{Config: cfg}},
		}})
		require.True(t, b.SinkConfigChanged())
		got, err := b.GetSinkConfig("my-sink")
		require.NoError(t, err)
		require.Equal(t, "my-sink", got.GetName())
	})

	t.Run("RemovedEventsSink → sinkConfigChanged", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_RemovedEventsSink{RemovedEventsSink: &commonpb.RemovedEventsSinkLog{Name: "gone"}},
		}})
		require.True(t, b.SinkConfigChanged())
	})

	t.Run("SetQueryCheckpointSchedule → queryCheckpointScheduleUpdate", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &commonpb.SetQueryCheckpointScheduleLog{Cron: "0 * * * *"}},
		}})
		require.NotNil(t, b.queryCheckpointScheduleUpdate)
		require.Equal(t, "0 * * * *", *b.queryCheckpointScheduleUpdate)
	})

	t.Run("DeleteQueryCheckpointSchedule → empty queryCheckpointScheduleUpdate", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeleteQueryCheckpointSchedule{DeleteQueryCheckpointSchedule: &commonpb.DeletedQueryCheckpointScheduleLog{}},
		}})
		require.NotNil(t, b.queryCheckpointScheduleUpdate)
		require.Empty(t, *b.queryCheckpointScheduleUpdate)
	})

	t.Run("DeleteLedger → deletedLedgers + mirrorConfigChanged", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Boundaries().Put(domain.LedgerKey{Name: "L"}, &raftcmdpb.LedgerBoundaries{NextTransactionId: 1})
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "L"}},
		}})
		// Absorb records signals only, never the overlay write: the Boundary
		// deletion moved to processDeleteLedger's gated Scope (EN-1522), so
		// Absorb must NOT touch the overlay.
		require.Equal(t, []string{"L"}, b.deletedLedgers)
		_, err := b.Boundaries().Get(domain.LedgerKey{Name: "L"})
		require.NoError(t, err, "Absorb must not delete the boundary — that is the gated handler's job")

		// Deleting a ledger drops it from ReadMirrorLedgers (which filters
		// DeletedAt == nil), so the mirror worker set must be reconciled.
		// Without this signal no ConfigChanged notification fires and a deleted
		// mirror ledger's worker keeps polling its v2 source. Set for every
		// deletion, not just mirrors: the mode is not reachable here without an
		// ungated overlay read, and a spurious reconcile is idempotent.
		require.True(t, b.MirrorConfigChanged())
	})

	t.Run("CreateLedger Mirror → mirrorConfigChanged", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  "mir",
			Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Mode: commonpb.LedgerMode_LEDGER_MODE_MIRROR}},
		}}}
		b.Absorb(order, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "mir"}},
		}})
		require.True(t, b.MirrorConfigChanged())
	})

	t.Run("CreateLedger Normal → no mirrorConfigChanged", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		order := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{
			Ledger:  "n",
			Payload: &raftcmdpb.LedgerScopedOrder_CreateLedger{CreateLedger: &raftcmdpb.CreateLedgerOrder{Mode: commonpb.LedgerMode_LEDGER_MODE_NORMAL}},
		}}}
		b.Absorb(order, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreateLedger{CreateLedger: &commonpb.CreatedLedgerLog{Name: "n"}},
		}})
		require.False(t, b.MirrorConfigChanged())
	})

	t.Run("PromoteLedger → mirrorConfigChanged", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_PromoteLedger{PromoteLedger: &commonpb.PromotedLedgerLog{Name: "p"}},
		}})
		require.True(t, b.MirrorConfigChanged())
	})

	t.Run("CreatedQueryCheckpoint → queryCheckpointCreated", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CreatedQueryCheckpoint{CreatedQueryCheckpoint: &commonpb.CreatedQueryCheckpointLog{CheckpointId: 99}},
		}})
		require.Equal(t, uint64(99), b.QueryCheckpointCreated())
	})

	t.Run("DeletedQueryCheckpoint → queryCheckpointDeleted", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeletedQueryCheckpoint{DeletedQueryCheckpoint: &commonpb.DeletedQueryCheckpointLog{CheckpointId: 100}},
		}})
		require.Equal(t, uint64(100), b.QueryCheckpointDeleted())
	})
}

// TestWriteSetAbsorb_NoOpForUnmappedPayloads pins that log payloads
// with no cross-order signal don't touch any accumulator field. State
// mutations for those payloads happen through Scope and are tested
// elsewhere.
func TestWriteSetAbsorb_NoOpForUnmappedPayloads(t *testing.T) {
	t.Parallel()

	cases := []*commonpb.LogPayload{
		{Type: &commonpb.LogPayload_Apply{Apply: &commonpb.ApplyLedgerLog{}}},
		{Type: &commonpb.LogPayload_SetMaintenanceMode{SetMaintenanceMode: &commonpb.SetMaintenanceModeLog{}}},
	}

	for _, p := range cases {
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: p})
		require.False(t, b.SinkConfigChanged())
		require.Nil(t, b.queryCheckpointScheduleUpdate)
		require.Empty(t, b.deletedLedgers)
		require.False(t, b.MirrorConfigChanged())
		require.Zero(t, b.QueryCheckpointCreated())
		require.Zero(t, b.QueryCheckpointDeleted())
	}
}
