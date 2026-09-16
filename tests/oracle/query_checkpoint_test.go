package oracle

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/domain"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/tests/oracle/oracletest"
)

func createCheckpointRequest() *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_CreateQueryCheckpoint{CreateQueryCheckpoint: &servicepb.CreateQueryCheckpointRequest{}}}
}
func deleteCheckpointRequest(id uint64) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpoint{DeleteQueryCheckpoint: &servicepb.DeleteQueryCheckpointRequest{CheckpointId: id}}}
}
func scheduleCheckpointRequest(cron string) *servicepb.Request {
	return &servicepb.Request{Type: &servicepb.Request_SetQueryCheckpointSchedule{SetQueryCheckpointSchedule: &servicepb.SetQueryCheckpointScheduleRequest{Cron: cron}}}
}
func TestGlobalState_QueryCheckpointLifecycle(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().WithQueryCheckpointLimit(1)
	created := base.Apply(keyedBulk("create", createCheckpointRequest()))
	require.True(t, created.OK)
	require.Equal(t, uint64(1), created.Orders[0].CheckpointID)
	require.Equal(t, []uint64{1}, created.State.QueryCheckpointIDs())
	require.Empty(t, base.QueryCheckpointIDs())
	require.NotEqual(t, base.Fingerprint(), created.State.Fingerprint())
	replay := created.State.Apply(keyedBulk("create", createCheckpointRequest()))
	require.True(t, replay.OK)
	require.Equal(t, created.State.Fingerprint(), replay.State.Fingerprint())
	require.Equal(t, uint64(1), replay.Orders[0].CheckpointID)
	limited := created.State.Apply(bulkOf(createCheckpointRequest()))
	require.False(t, limited.OK)
	require.Equal(t, domain.ErrReasonCheckpointLimitReached, limited.Reason)
	require.Equal(t, created.State.Fingerprint(), limited.State.Fingerprint())
	deleted := created.State.Apply(keyedBulk("delete", deleteCheckpointRequest(1)))
	require.True(t, deleted.OK)
	require.Empty(t, deleted.State.QueryCheckpointIDs())
	require.Equal(t, []uint64{1}, created.State.QueryCheckpointIDs())
	require.True(t, deleted.State.Apply(keyedBulk("delete", deleteCheckpointRequest(1))).OK)
	absent := deleted.State.Apply(bulkOf(deleteCheckpointRequest(1)))
	require.Equal(t, domain.ErrReasonCheckpointNotFound, absent.Reason)
	second := deleted.State.Apply(bulkOf(createCheckpointRequest()))
	require.True(t, second.OK)
	require.Equal(t, uint64(2), second.Orders[0].CheckpointID)
	require.Equal(t, uint64(1), second.State.QueryCheckpointLimit())
}
func TestGlobalState_QueryCheckpointAtomicityAndSchedule(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().WithQueryCheckpointLimit(1)
	rejected := base.Apply(bulkOf(createCheckpointRequest(), createCheckpointRequest()))
	require.False(t, rejected.OK)
	require.Equal(t, base.Fingerprint(), rejected.State.Fingerprint())
	require.Equal(t, uint64(1), rejected.State.Apply(bulkOf(createCheckpointRequest())).Orders[0].CheckpointID)
	set := base.Apply(bulkOf(scheduleCheckpointRequest("0 0 1 1 *")))
	require.True(t, set.OK)
	require.Equal(t, "0 0 1 1 *", set.State.QueryCheckpointSchedule())
	require.Empty(t, base.QueryCheckpointSchedule())
	require.NotEqual(t, base.Fingerprint(), set.State.Fingerprint())
	invalid := set.State.Apply(bulkOf(scheduleCheckpointRequest("invalid")))
	require.Equal(t, domain.ErrReasonInvalidCronExpression, invalid.Reason)
	require.Equal(t, set.State.Fingerprint(), invalid.State.Fingerprint())
	del := &servicepb.Request{Type: &servicepb.Request_DeleteQueryCheckpointSchedule{DeleteQueryCheckpointSchedule: &servicepb.DeleteQueryCheckpointScheduleRequest{}}}
	cleared := set.State.Apply(bulkOf(del, del))
	require.True(t, cleared.OK)
	require.Empty(t, cleared.State.QueryCheckpointSchedule())
	require.Equal(t, base.Fingerprint(), cleared.State.Fingerprint())
	require.NotEqual(t, base.Fingerprint(), base.WithQueryCheckpointLimit(2).Fingerprint())
	require.Equal(t, domain.ErrReasonCheckpointIDRequired, base.Apply(bulkOf(deleteCheckpointRequest(0))).Reason)
}

func TestGlobalState_QueryCheckpointMixedBulk(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().WithQueryCheckpointLimit(1)
	committed := base.Apply(bulkOf(oracletest.AddTypeReq("T"), createCheckpointRequest()))
	require.True(t, committed.OK)
	require.True(t, committed.State.QueryCheckpointExists(1))
	require.Equal(t, 1, committed.State.Ledger("L").logs.Len())
	require.Len(t, committed.State.ledgers, 1)
	rejected := base.Apply(bulkOf(createCheckpointRequest(), oracletest.AddTypeReq("T"), oracletest.AddTypeReq("T")))
	require.False(t, rejected.OK)
	require.Equal(t, domain.ErrReasonAccountTypeAlreadyExists, rejected.Reason)
	require.Equal(t, base.Fingerprint(), rejected.State.Fingerprint())
	require.Empty(t, rejected.State.QueryCheckpointIDs())
	require.True(t, rejected.State.Ledger("L").IsEmpty())
}

func TestGlobalState_SeedQueryCheckpoints(t *testing.T) {
	t.Parallel()
	base := NewGlobalState().WithQueryCheckpointLimit(2)
	seeded := base.SeedQueryCheckpoints([]uint64{3}, 8)
	require.Equal(t, []uint64{3}, seeded.QueryCheckpointIDs())
	require.Empty(t, base.QueryCheckpointIDs())
	require.Equal(t, uint64(2), seeded.QueryCheckpointLimit())
	require.Empty(t, seeded.QueryCheckpointSchedule())
	created := seeded.Apply(bulkOf(createCheckpointRequest()))
	require.True(t, created.OK)
	require.Equal(t, uint64(8), created.Orders[0].CheckpointID)
	require.Equal(t, domain.ErrReasonCheckpointLimitReached, created.State.Apply(bulkOf(createCheckpointRequest())).Reason)
	require.NotEqual(t, seeded.Fingerprint(), base.Fingerprint())
	require.Equal(t, uint64(9), base.SeedQueryCheckpoints(nil, 9).Apply(bulkOf(createCheckpointRequest())).Orders[0].CheckpointID)
}
