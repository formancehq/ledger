package node

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3"
	"go.etcd.io/raft/v3/raftpb"
	"google.golang.org/protobuf/proto"
)

func TestLeadershipGainTarget(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name    string
		rd      raft.Ready
		want    uint64
		wantErr error
	}{
		{
			name:    "empty entries surfaces the contract violation",
			rd:      raft.Ready{},
			wantErr: errEmptyLeadershipGainReady,
		},
		{
			name: "single entry (the no-op) gives its own index",
			rd: raft.Ready{
				Entries: []*raftpb.Entry{{Index: proto.Uint64(42)}},
			},
			want: 42,
		},
		{
			name: "multiple entries — last index wins (the no-op is appended last)",
			rd: raft.Ready{
				Entries: []*raftpb.Entry{{Index: proto.Uint64(10)}, {Index: proto.Uint64(11)}, {Index: proto.Uint64(12)}},
			},
			want: 12,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			got, err := leadershipGainTarget(tc.rd)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)

				return
			}

			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}
