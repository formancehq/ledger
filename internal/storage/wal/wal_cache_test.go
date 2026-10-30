package wal

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
	"go.etcd.io/raft/v3/raftpb"
)

func TestEntryCacheIsBoundedAndReloadsEvictedEntries(t *testing.T) {
	w := newTestWAL(t, WithEntryCacheBytes(128))
	entries := make([]*raftpb.Entry, 32)
	for i := range entries {
		index := uint64(i + 1)
		entries[i] = ent(index, 1, bytes.Repeat([]byte{byte(index)}, 64))
	}
	require.NoError(t, w.Append(hs(1, 1, 32), entries))

	require.Less(t, len(w.entries), len(entries), "payload cache must evict entries over its byte budget")
	loaded, err := w.Entries(1, 33, ^uint64(0))
	require.NoError(t, err)
	require.Len(t, loaded, len(entries))
	for i, entry := range loaded {
		require.Equal(t, entries[i].GetIndex(), entry.GetIndex())
		require.Equal(t, entries[i].GetData(), entry.GetData())
	}
}
