package raft

import (
	"testing"

	"github.com/formancehq/go-libs/v3/logging"
	"github.com/formancehq/ledger-v3-poc/internal/ledgerpb"
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

	wal, err := NewWAL(t.TempDir(), logging.FromContext(ctx))
	require.NoError(t, err)

	syncer := newSyncer(
		spool,
		fsm,
		logging.FromContext(ctx),
		wal,
		noop.Meter{},
		10, 10,
	)
	go syncer.run()
	t.Cleanup(syncer.stop)

	cmd := &ledgerpb.Command{
		Type: ledgerpb.CommandType_CreateLedger,
	}

	fsm.EXPECT().
		ApplyEntries(gomock.Any(), cmd).
		Return([]ApplyResult{{}}, nil)

	_, err = syncer.ApplyEntries(ctx, 10, &raftpb.ConfState{}, cmd)
	require.NoError(t, err)
}
