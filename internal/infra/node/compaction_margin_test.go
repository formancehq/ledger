package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
	"go.uber.org/mock/gomock"

	"github.com/formancehq/ledger/v3/internal/storage/wal"
)

func TestCompactionIndexHonorsEntryCountForSmallEntries(t *testing.T) {
	t.Parallel()

	mockWAL := wal.NewMockWAL(gomock.NewController(t))
	mockWAL.EXPECT().FirstIndex().Return(uint64(1), nil)
	mockWAL.EXPECT().Entries(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(lo, hi, _ uint64) ([]*raftpb.Entry, error) {
		return []*raftpb.Entry{{Index: lo, Data: []byte("small")}}, nil
	}).AnyTimes()

	compactAt, err := compactionIndex(mockWAL, 10, 3)
	require.NoError(t, err)
	require.Equal(t, uint64(7), compactAt)
}

func TestCompactionIndexBoundsLargeEntrySuffixByBytes(t *testing.T) {
	t.Parallel()

	mockWAL := wal.NewMockWAL(gomock.NewController(t))
	mockWAL.EXPECT().FirstIndex().Return(uint64(1), nil)
	mockWAL.EXPECT().Entries(gomock.Any(), gomock.Any(), gomock.Any()).DoAndReturn(func(lo, hi, _ uint64) ([]*raftpb.Entry, error) {
		return []*raftpb.Entry{{Index: lo, Data: make([]byte, 10<<20)}}, nil
	}).AnyTimes()

	compactAt, err := compactionIndex(mockWAL, 1000, 1000)
	require.NoError(t, err)
	// 25 x 10 MiB fit under the 256 MiB cap; the next entry would exceed it.
	require.Equal(t, uint64(975), compactAt)
}
