package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestClassifyCheckpointOrderPosition(t *testing.T) {
	t.Parallel()

	apply := func() *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "l", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{}}}}}
	}
	chkpt := func() *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
					CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
				},
			},
		}}
	}
	closeChapter := func() *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
			SystemScoped: &raftcmdpb.SystemScopedOrder{
				Payload: &raftcmdpb.SystemScopedOrder_CloseChapter{
					CloseChapter: &raftcmdpb.CloseChapterOrder{},
				},
			},
		}}
	}

	cases := []struct {
		name   string
		orders []*raftcmdpb.Order
		want   CheckpointOrderPosition
	}{
		{"empty", nil, CheckpointOrderAbsent},
		{"only apply", []*raftcmdpb.Order{apply(), apply()}, CheckpointOrderAbsent},
		{"checkpoint alone", []*raftcmdpb.Order{chkpt()}, CheckpointOrderLast},
		{"close chapter alone", []*raftcmdpb.Order{closeChapter()}, CheckpointOrderLast},
		{"apply then checkpoint", []*raftcmdpb.Order{apply(), apply(), chkpt()}, CheckpointOrderLast},
		{"apply then close chapter", []*raftcmdpb.Order{apply(), closeChapter()}, CheckpointOrderLast},
		{"checkpoint then apply", []*raftcmdpb.Order{chkpt(), apply()}, CheckpointOrderInvalid},
		{"close chapter then apply", []*raftcmdpb.Order{closeChapter(), apply()}, CheckpointOrderInvalid},
		{"checkpoint mid-batch", []*raftcmdpb.Order{apply(), chkpt(), apply()}, CheckpointOrderInvalid},
		// Two triggers: only the LAST slot is valid. The first trigger at
		// position 0 already violates the invariant, regardless of what
		// follows. (Submitting two checkpoint orders in one proposal is
		// nonsensical anyway, but the validator must reject it cleanly.)
		{"two checkpoints", []*raftcmdpb.Order{chkpt(), chkpt()}, CheckpointOrderInvalid},
		{"close chapter then checkpoint", []*raftcmdpb.Order{closeChapter(), chkpt()}, CheckpointOrderInvalid},
		{"checkpoint then close chapter", []*raftcmdpb.Order{chkpt(), closeChapter()}, CheckpointOrderInvalid},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			require.Equal(t, tc.want, ClassifyCheckpointOrderPosition(tc.orders))
		})
	}
}

func TestValidateCheckpointEntryPositions(t *testing.T) {
	t.Parallel()

	apply := func(t *testing.T, idx uint64) *raftpb.Entry {
		t.Helper()
		cmd := &raftcmdpb.Proposal{
			Id:     idx,
			Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "l", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{}}}}}},
		}
		data, err := cmd.MarshalVT()
		require.NoError(t, err)

		return &raftpb.Entry{Index: new(idx), Term: proto.Uint64(1), Type: new(raftpb.EntryNormal), Data: data}
	}
	chkpt := func(t *testing.T, idx uint64) *raftpb.Entry {
		t.Helper()
		cmd := &raftcmdpb.Proposal{
			Id: idx,
			Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_SystemScoped{
				SystemScoped: &raftcmdpb.SystemScopedOrder{
					Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
						CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
					},
				},
			}}},
		}
		data, err := cmd.MarshalVT()
		require.NoError(t, err)

		return &raftpb.Entry{Index: new(idx), Term: proto.Uint64(1), Type: new(raftpb.EntryNormal), Data: data}
	}

	confChange := &raftpb.Entry{Index: proto.Uint64(99), Term: proto.Uint64(1), Type: new(raftpb.EntryConfChange)}
	emptyData := &raftpb.Entry{Index: proto.Uint64(99), Term: proto.Uint64(1), Type: new(raftpb.EntryNormal)}

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions(nil))
	})
	t.Run("no trigger", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions([]*raftpb.Entry{apply(t, 1), apply(t, 2)}))
	})
	t.Run("trigger last", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions([]*raftpb.Entry{apply(t, 1), chkpt(t, 2)}))
	})
	t.Run("single trigger", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions([]*raftpb.Entry{chkpt(t, 1)}))
	})
	t.Run("trigger first", func(t *testing.T) {
		t.Parallel()
		err := ValidateCheckpointEntryPositions([]*raftpb.Entry{chkpt(t, 1), apply(t, 2)})
		require.Error(t, err)
		require.Contains(t, err.Error(), "applier must pre-split")
	})
	t.Run("trigger middle", func(t *testing.T) {
		t.Parallel()
		err := ValidateCheckpointEntryPositions([]*raftpb.Entry{apply(t, 1), chkpt(t, 2), apply(t, 3)})
		require.Error(t, err)
	})
	t.Run("conf-change and empty entries are skipped", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions([]*raftpb.Entry{confChange, emptyData, chkpt(t, 100)}))
	})
}

func TestProposalRequiresCheckpoint(t *testing.T) {
	t.Parallel()

	apply := &raftcmdpb.Order{Type: &raftcmdpb.Order_LedgerScoped{LedgerScoped: &raftcmdpb.LedgerScopedOrder{Ledger: "l", Payload: &raftcmdpb.LedgerScopedOrder_Apply{Apply: &raftcmdpb.LedgerApplyOrder{}}}}}
	chkpt := &raftcmdpb.Order{Type: &raftcmdpb.Order_SystemScoped{
		SystemScoped: &raftcmdpb.SystemScopedOrder{
			Payload: &raftcmdpb.SystemScopedOrder_CreateQueryCheckpoint{
				CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{},
			},
		},
	}}

	require.False(t, ProposalRequiresCheckpoint(&raftcmdpb.Proposal{}))
	require.False(t, ProposalRequiresCheckpoint(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{apply}}))
	require.True(t, ProposalRequiresCheckpoint(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{apply, chkpt}}))
	require.True(t, ProposalRequiresCheckpoint(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{chkpt}}))
}
