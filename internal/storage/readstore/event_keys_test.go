package readstore

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"
)

// eventKey builds [prefix][group]\x00[seq BE 8][op] the way
// MetadataIndexEventKeyV and the EntityExists* builders do.
func eventKey(prefix, group []byte, seq uint64, op byte) []byte {
	key := append(append([]byte(nil), prefix...), group...)
	key = append(key, metadataEventTerminator)
	key = binary.BigEndian.AppendUint64(key, seq)

	return append(key, op)
}

// TestParseEventKey pins the layout contract both EventResolveIterator and
// ReverseEventResolveIterator read through. The reject arms matter as much as
// the accept arm: a key this package would not have written must come back
// ok=false so the caller raises a loud error instead of resolving a group
// from bytes it cannot read.
func TestParseEventKey(t *testing.T) {
	t.Parallel()

	prefix := []byte("PFX")

	tests := []struct {
		name      string
		key       []byte
		prefixLen int
		wantOK    bool
		wantGroup []byte
		wantSeq   uint64
		wantOp    byte
	}{
		{
			name:      "add event",
			key:       eventKey(prefix, []byte("acc:alice"), 42, MetadataEventAdd),
			prefixLen: len(prefix),
			wantOK:    true,
			wantGroup: []byte("acc:alice"),
			wantSeq:   42,
			wantOp:    MetadataEventAdd,
		},
		{
			name:      "del event",
			key:       eventKey(prefix, []byte("acc:bob"), 7, MetadataEventDel),
			prefixLen: len(prefix),
			wantOK:    true,
			wantGroup: []byte("acc:bob"),
			wantSeq:   7,
			wantOp:    MetadataEventDel,
		},
		{
			name:      "empty group",
			key:       eventKey(prefix, nil, 1, MetadataEventAdd),
			prefixLen: len(prefix),
			wantOK:    true,
			wantGroup: []byte{},
			wantSeq:   1,
			wantOp:    MetadataEventAdd,
		},
		{
			name:      "group with embedded terminator byte",
			key:       eventKey(prefix, []byte{'a', 0x00, 'b'}, 9, MetadataEventAdd),
			prefixLen: len(prefix),
			wantOK:    true,
			wantGroup: []byte{'a', 0x00, 'b'},
			wantSeq:   9,
			wantOp:    MetadataEventAdd,
		},
		{
			name:      "unknown op rejected",
			key:       eventKey(prefix, []byte("acc:alice"), 42, 0x7f),
			prefixLen: len(prefix),
			wantOK:    false,
		},
		{
			name:      "missing terminator rejected",
			key:       append(append(append([]byte(nil), prefix...), []byte("acc:alice")...), append([]byte{0x01}, make([]byte, metadataEventSuffixLen-1)...)...),
			prefixLen: len(prefix),
			wantOK:    false,
		},
		{
			name:      "suffix shorter than the fixed tail rejected",
			key:       append(append([]byte(nil), prefix...), make([]byte, metadataEventSuffixLen)...),
			prefixLen: len(prefix),
			wantOK:    false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			group, seq, op, ok := parseEventKey(test.key, test.prefixLen)
			require.Equal(t, test.wantOK, ok)

			if !test.wantOK {
				require.Nil(t, group)
				require.Zero(t, seq)
				require.Zero(t, op)

				return
			}

			require.Equal(t, test.wantGroup, group)
			require.Equal(t, test.wantSeq, seq)
			require.Equal(t, test.wantOp, op)
		})
	}
}
