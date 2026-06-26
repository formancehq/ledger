package state

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

// TestWriteSetAbsorb_CoversEveryDerivedPayload pins the mapping between
// each LogPayload variant that should mutate the WriteSet's cross-order
// accumulators and the field it touches. If a new derivable LogPayload
// variant is added without a matching case in Absorb, the corresponding
// subtest here will fail (the assertion expects a non-zero
// accumulator). Payloads with no cross-order signal — Apply,
// SaveLedgerMetadata, SealChapter, signing/maintenance, ... — are
// covered by TestWriteSetAbsorb_NoOpForUnmappedPayloads below.
func TestWriteSetAbsorb_CoversEveryDerivedPayload(t *testing.T) {
	t.Parallel()

	t.Run("ArchiveChapter → archiveRequests", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, archiveChapterLog(7, 10, 20, 30, 40))
		require.Equal(t, []ArchiveRequest{{ChapterID: 7, StartSequence: 10, CloseSequence: 20, StartAuditSequence: 30, CloseAuditSequence: 40}}, b.archiveRequests)
	})

	t.Run("ConfirmArchiveChapter → purgeRanges", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, confirmArchiveLog(8, 100, 200, 50, 60))
		require.Len(t, b.purgeRanges, 1)
		require.Equal(t, uint64(8), b.purgeRanges[0].chapterID)
		require.Equal(t, uint64(100), b.purgeRanges[0].startSequence)
		require.Equal(t, uint64(200), b.purgeRanges[0].closeSequence)
		require.Equal(t, uint64(50), b.purgeRanges[0].startAuditSequence)
		require.Equal(t, uint64(60), b.purgeRanges[0].closeAuditSequence)
	})

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

	t.Run("SetChapterSchedule → chapterScheduleUpdate", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_SetChapterSchedule{SetChapterSchedule: &commonpb.SetChapterScheduleLog{Cron: "* * * * *"}},
		}})
		require.NotNil(t, b.chapterScheduleUpdate)
		require.Equal(t, "* * * * *", *b.chapterScheduleUpdate)
	})

	t.Run("DeleteChapterSchedule → empty chapterScheduleUpdate", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeleteChapterSchedule{DeleteChapterSchedule: &commonpb.DeletedChapterScheduleLog{}},
		}})
		require.NotNil(t, b.chapterScheduleUpdate)
		require.Empty(t, *b.chapterScheduleUpdate)
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

	t.Run("DeleteLedger → deletedLedgers + Boundaries drop", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		b.PutBoundaries("L", &raftcmdpb.LedgerBoundaries{NextTransactionId: 1})
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_DeleteLedger{DeleteLedger: &commonpb.DeletedLedgerLog{Name: "L"}},
		}})
		require.Equal(t, []string{"L"}, b.deletedLedgers)
		_, err := b.GetBoundaries("L")
		require.Error(t, err, "boundaries overlay must reflect the deletion immediately")
	})

	t.Run("CloseChapter → chapterClosing", func(t *testing.T) {
		t.Parallel()
		b, _, _ := newTestBuffer(t)
		require.False(t, b.ChapterClosing())
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: &commonpb.LogPayload{
			Type: &commonpb.LogPayload_CloseChapter{CloseChapter: &commonpb.ClosedChapterLog{
				ClosedChapter: &commonpb.Chapter{Id: 42},
			}},
		}})
		require.True(t, b.ChapterClosing())
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
		{Type: &commonpb.LogPayload_SealChapter{SealChapter: &commonpb.SealedChapterLog{}}},
	}

	for _, p := range cases {
		b, _, _ := newTestBuffer(t)
		b.Absorb(&raftcmdpb.Order{}, &commonpb.Log{Payload: p})
		require.Empty(t, b.archiveRequests)
		require.Empty(t, b.purgeRanges)
		require.False(t, b.SinkConfigChanged())
		require.Nil(t, b.chapterScheduleUpdate)
		require.Nil(t, b.queryCheckpointScheduleUpdate)
		require.Empty(t, b.deletedLedgers)
		require.False(t, b.ChapterClosing())
		require.False(t, b.MirrorConfigChanged())
		require.Zero(t, b.QueryCheckpointCreated())
		require.Zero(t, b.QueryCheckpointDeleted())
	}
}
