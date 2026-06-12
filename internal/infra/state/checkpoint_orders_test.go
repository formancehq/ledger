package state

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestClassifyCheckpointOrderPosition(t *testing.T) {
	t.Parallel()

	apply := func() *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_Apply{Apply: &raftcmdpb.LedgerApplyOrder{}}}
	}
	chkpt := func() *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}}}
	}
	closePeriod := func() *raftcmdpb.Order {
		return &raftcmdpb.Order{Type: &raftcmdpb.Order_ClosePeriod{ClosePeriod: &raftcmdpb.ClosePeriodOrder{}}}
	}

	cases := []struct {
		name   string
		orders []*raftcmdpb.Order
		want   CheckpointOrderPosition
	}{
		{"empty", nil, CheckpointOrderAbsent},
		{"only apply", []*raftcmdpb.Order{apply(), apply()}, CheckpointOrderAbsent},
		{"checkpoint alone", []*raftcmdpb.Order{chkpt()}, CheckpointOrderLast},
		{"close period alone", []*raftcmdpb.Order{closePeriod()}, CheckpointOrderLast},
		{"apply then checkpoint", []*raftcmdpb.Order{apply(), apply(), chkpt()}, CheckpointOrderLast},
		{"apply then close period", []*raftcmdpb.Order{apply(), closePeriod()}, CheckpointOrderLast},
		{"checkpoint then apply", []*raftcmdpb.Order{chkpt(), apply()}, CheckpointOrderInvalid},
		{"close period then apply", []*raftcmdpb.Order{closePeriod(), apply()}, CheckpointOrderInvalid},
		{"checkpoint mid-batch", []*raftcmdpb.Order{apply(), chkpt(), apply()}, CheckpointOrderInvalid},
		// Two triggers: only the LAST slot is valid. The first trigger at
		// position 0 already violates the invariant, regardless of what
		// follows. (Submitting two checkpoint orders in one proposal is
		// nonsensical anyway, but the validator must reject it cleanly.)
		{"two checkpoints", []*raftcmdpb.Order{chkpt(), chkpt()}, CheckpointOrderInvalid},
		{"close period then checkpoint", []*raftcmdpb.Order{closePeriod(), chkpt()}, CheckpointOrderInvalid},
		{"checkpoint then close period", []*raftcmdpb.Order{chkpt(), closePeriod()}, CheckpointOrderInvalid},
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

	apply := func(t *testing.T, idx uint64) raftpb.Entry {
		t.Helper()
		cmd := &raftcmdpb.Proposal{
			Id:     idx,
			Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_Apply{Apply: &raftcmdpb.LedgerApplyOrder{}}}},
		}
		data, err := cmd.MarshalVT()
		require.NoError(t, err)

		return raftpb.Entry{Index: idx, Term: 1, Type: raftpb.EntryNormal, Data: data}
	}
	chkpt := func(t *testing.T, idx uint64) raftpb.Entry {
		t.Helper()
		cmd := &raftcmdpb.Proposal{
			Id:     idx,
			Orders: []*raftcmdpb.Order{{Type: &raftcmdpb.Order_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}}}},
		}
		data, err := cmd.MarshalVT()
		require.NoError(t, err)

		return raftpb.Entry{Index: idx, Term: 1, Type: raftpb.EntryNormal, Data: data}
	}

	confChange := raftpb.Entry{Index: 99, Term: 1, Type: raftpb.EntryConfChange}
	emptyData := raftpb.Entry{Index: 99, Term: 1, Type: raftpb.EntryNormal}

	t.Run("empty slice", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions(nil))
	})
	t.Run("no trigger", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions([]raftpb.Entry{apply(t, 1), apply(t, 2)}))
	})
	t.Run("trigger last", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions([]raftpb.Entry{apply(t, 1), chkpt(t, 2)}))
	})
	t.Run("single trigger", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions([]raftpb.Entry{chkpt(t, 1)}))
	})
	t.Run("trigger first", func(t *testing.T) {
		t.Parallel()
		err := ValidateCheckpointEntryPositions([]raftpb.Entry{chkpt(t, 1), apply(t, 2)})
		require.Error(t, err)
		require.Contains(t, err.Error(), "applier must pre-split")
	})
	t.Run("trigger middle", func(t *testing.T) {
		t.Parallel()
		err := ValidateCheckpointEntryPositions([]raftpb.Entry{apply(t, 1), chkpt(t, 2), apply(t, 3)})
		require.Error(t, err)
	})
	t.Run("conf-change and empty entries are skipped", func(t *testing.T) {
		t.Parallel()
		require.NoError(t, ValidateCheckpointEntryPositions([]raftpb.Entry{confChange, emptyData, chkpt(t, 100)}))
	})
}

func TestProposalRequiresCheckpoint(t *testing.T) {
	t.Parallel()

	apply := &raftcmdpb.Order{Type: &raftcmdpb.Order_Apply{Apply: &raftcmdpb.LedgerApplyOrder{}}}
	chkpt := &raftcmdpb.Order{Type: &raftcmdpb.Order_CreateQueryCheckpoint{CreateQueryCheckpoint: &raftcmdpb.CreateQueryCheckpointOrder{}}}

	require.False(t, ProposalRequiresCheckpoint(&raftcmdpb.Proposal{}))
	require.False(t, ProposalRequiresCheckpoint(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{apply}}))
	require.True(t, ProposalRequiresCheckpoint(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{apply, chkpt}}))
	require.True(t, ProposalRequiresCheckpoint(&raftcmdpb.Proposal{Orders: []*raftcmdpb.Order{chkpt}}))
}
