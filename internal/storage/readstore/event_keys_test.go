package readstore

import (
	"encoding/binary"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

type eventRowFixture struct {
	key        []byte
	value      []byte
	valueReads int
}

func (f *eventRowFixture) Key() []byte { return f.key }
func (f *eventRowFixture) Value() []byte {
	f.valueReads++

	return f.value
}

func TestParseIndexEventRowAcrossZones(t *testing.T) {
	t.Parallel()

	kb := dal.NewKeyBuilder()
	assetPrefix := AccountByAssetPrefix(kb, "ledger", "USD", 2)
	legacyKey := legacyAccountAssetKey(kb, "a")
	tests := []struct {
		name           string
		key            []byte
		value          []byte
		prefixLen      int
		wantGroup      []byte
		wantSeq        uint64
		wantOp         byte
		wantLegacy     bool
		wantOK         bool
		wantValueReads int
	}{
		{
			name:      "metadata DEL",
			key:       eventKey([]byte{PrefixMetadataIndex}, []byte("a"), 5, IndexEventDel),
			prefixLen: 1, wantGroup: []byte("a"), wantSeq: 5, wantOp: IndexEventDel, wantOK: true,
		},
		{
			name:      "exists ADD",
			key:       eventKey([]byte{PrefixEntityExists}, []byte("b"), 7, IndexEventAdd),
			prefixLen: 1, wantGroup: []byte("b"), wantSeq: 7, wantOp: IndexEventAdd, wantOK: true,
		},
		{
			name:      "account asset event",
			key:       AccountByAssetEventKey(kb, "ledger", "USD", 2, "a", 9),
			prefixLen: len(assetPrefix), wantGroup: []byte("a"), wantSeq: 9, wantOp: IndexEventAdd, wantOK: true,
		},
		{
			name: "account asset legacy row",
			key:  legacyKey, value: binary.BigEndian.AppendUint64(nil, 11),
			prefixLen: len(assetPrefix), wantGroup: []byte("a"), wantSeq: 11, wantOp: IndexEventAdd,
			wantLegacy: true, wantOK: true, wantValueReads: 1,
		},
		{
			name:      "account asset DEL rejected",
			key:       eventKey(assetPrefix, []byte("a"), 9, IndexEventDel),
			prefixLen: len(assetPrefix),
		},
		{
			name: "malformed legacy stamp rejected",
			key:  legacyKey, value: []byte{1}, prefixLen: len(assetPrefix), wantValueReads: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			fixture := &eventRowFixture{key: test.key, value: test.value}
			row, ok := parseIndexEventRow(fixture, test.prefixLen)
			require.Equal(t, test.wantOK, ok)
			require.Equal(t, test.wantValueReads, fixture.valueReads)
			if !ok {
				require.Equal(t, indexEventRow{}, row)

				return
			}

			require.Equal(t, test.wantGroup, row.group)
			require.Equal(t, test.wantSeq, row.seq)
			require.Equal(t, test.wantOp, row.op)
			require.Equal(t, test.wantLegacy, row.legacy)
		})
	}
}

func BenchmarkParseIndexEventRow(b *testing.B) {
	kb := dal.NewKeyBuilder()
	assetPrefix := AccountByAssetPrefix(kb, "ledger", "USD", 2)
	for _, test := range []struct {
		name      string
		key       []byte
		value     []byte
		prefixLen int
	}{
		{"metadata", eventKey([]byte{PrefixMetadataIndex}, []byte("accounts:alice"), 42, IndexEventAdd), nil, 1},
		{"asset event", AccountByAssetEventKey(kb, "ledger", "USD", 2, "accounts:alice", 42), nil, len(assetPrefix)},
		{"RC.5 asset row", legacyAccountAssetKey(kb, "accounts:alice"), binary.BigEndian.AppendUint64(nil, 42), len(assetPrefix)},
	} {
		b.Run(test.name, func(b *testing.B) {
			source := &eventRowFixture{key: test.key, value: test.value}
			b.ReportAllocs()
			for b.Loop() {
				row, ok := parseIndexEventRow(source, test.prefixLen)
				if !ok || row.seq != 42 {
					b.Fatal("event row did not decode")
				}
			}
		})
	}
}

// eventKey builds [prefix][group]\x00[seq BE 8][op] the way
// MetadataIndexEventKeyV and the EntityExists* builders do.
func eventKey(prefix, group []byte, seq uint64, op byte) []byte {
	key := append(append([]byte(nil), prefix...), group...)
	key = append(key, indexEventTerminator)
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
			key:       eventKey(prefix, []byte("acc:alice"), 42, IndexEventAdd),
			prefixLen: len(prefix),
			wantOK:    true,
			wantGroup: []byte("acc:alice"),
			wantSeq:   42,
			wantOp:    IndexEventAdd,
		},
		{
			name:      "del event",
			key:       eventKey(prefix, []byte("acc:bob"), 7, IndexEventDel),
			prefixLen: len(prefix),
			wantOK:    true,
			wantGroup: []byte("acc:bob"),
			wantSeq:   7,
			wantOp:    IndexEventDel,
		},
		{
			name:      "empty group",
			key:       eventKey(prefix, nil, 1, IndexEventAdd),
			prefixLen: len(prefix),
			wantOK:    true,
			wantGroup: []byte{},
			wantSeq:   1,
			wantOp:    IndexEventAdd,
		},
		{
			name:      "group with embedded terminator byte",
			key:       eventKey(prefix, []byte{'a', 0x00, 'b'}, 9, IndexEventAdd),
			prefixLen: len(prefix),
			wantOK:    true,
			wantGroup: []byte{'a', 0x00, 'b'},
			wantSeq:   9,
			wantOp:    IndexEventAdd,
		},
		{
			name:      "unknown op rejected",
			key:       eventKey(prefix, []byte("acc:alice"), 42, 0x7f),
			prefixLen: len(prefix),
			wantOK:    false,
		},
		{
			name:      "missing terminator rejected",
			key:       append(append(append([]byte(nil), prefix...), []byte("acc:alice")...), append([]byte{0x01}, make([]byte, indexEventSuffixLen-1)...)...),
			prefixLen: len(prefix),
			wantOK:    false,
		},
		{
			name:      "suffix shorter than the fixed tail rejected",
			key:       append(append([]byte(nil), prefix...), make([]byte, indexEventSuffixLen)...),
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
