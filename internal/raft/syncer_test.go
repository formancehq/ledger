package raft

import (
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/wal"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/mock/gomock"
)

func TestSyncerApplyEntries(t *testing.T) {
	t.Parallel()

	ctx := logging.TestingContext()

	ctrl := gomock.NewController(t)
	fsm := NewMockFSM(ctrl)
	spool := NewMockSpool(ctrl)

	wal, err := wal.New(t.TempDir(), logging.FromContext(ctx))
	require.NoError(t, err)

	syncer := newSyncer(
		spool,
		fsm,
		logging.FromContext(ctx),
		wal,
		noop.Meter{},
		10, 10,
	)

	fsm.EXPECT().
		ApplyEntries(gomock.Any(), raftpb.Entry{
			Index: 1,
		}).
		Return([]ApplyResult{{}}, nil)

	_, err = syncer.ApplyEntries(ctx, &raftpb.ConfState{}, raftpb.Entry{
		Index: 1,
	})
	require.NoError(t, err)
}
