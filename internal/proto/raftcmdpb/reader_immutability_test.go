package raftcmdpb_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/raftcmdpb"
)

func TestIdempotencyEvictionReader_GetPebbleKeyHashes_DeepClones(t *testing.T) {
	t.Parallel()

	h1 := []byte{0x01, 0x02}
	h2 := []byte{0x03, 0x04}
	ev := &raftcmdpb.IdempotencyEviction{PebbleKeyHashes: [][]byte{h1, h2}}
	r := ev.AsReader()

	got := r.GetPebbleKeyHashes()
	require.Len(t, got, 2)

	// Mutate the outer slice: must not affect the original.
	got[0] = []byte{0xFF}
	require.Equal(t, []byte{0x01, 0x02}, ev.GetPebbleKeyHashes()[0])

	// Mutate inner []byte: must not affect the original either (deep clone).
	got2 := r.GetPebbleKeyHashes()
	got2[1][0] = 0xEE
	require.Equal(t, []byte{0x03, 0x04}, ev.GetPebbleKeyHashes()[1])
	require.Equal(t, []byte{0x03, 0x04}, h2)
}

func TestIdempotencyEvictionReader_GetLastScannedTimeIndexKey_ClonesBytes(t *testing.T) {
	t.Parallel()

	ev := &raftcmdpb.IdempotencyEviction{LastScannedTimeIndexKey: []byte{0x10, 0x20, 0x30}}
	r := ev.AsReader()

	got := r.GetLastScannedTimeIndexKey()
	got[0] = 0xFF
	require.Equal(t, []byte{0x10, 0x20, 0x30}, ev.GetLastScannedTimeIndexKey())
}
