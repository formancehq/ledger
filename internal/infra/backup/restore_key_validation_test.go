package backup

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"

	"github.com/formancehq/ledger/v3/internal/storage/dal"
)

func TestApplyExportsRejectsKeyOutsideSegmentKeyspace(t *testing.T) {
	var stream bytes.Buffer
	writer := NewKVStreamWriter(&stream)
	require.NoError(t, writer.WriteHeader())
	require.NoError(t, writer.WriteEntry([]byte{0xff, 0x01, 'x'}, []byte("value")))
	require.NoError(t, writer.WriteFooter())

	store := newBackupTestStore(t)
	storage := &recordingStorage{manifestBody: stream.Bytes()}
	err := ApplyExports(context.Background(), logging.Testing(), storage, store, []ExportSegment{
		{Type: "log", StartSeq: 1, EndSeq: 1, Key: "invalid", Size: int64(stream.Len())},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid key")
}

func TestApplyExportsRejectsUnsupportedEmptySegment(t *testing.T) {
	t.Parallel()

	var stream bytes.Buffer
	writer := NewKVStreamWriter(&stream)
	require.NoError(t, writer.WriteHeader())
	require.NoError(t, writer.WriteFooter())

	store := newBackupTestStore(t)
	storage := &recordingStorage{manifestBody: stream.Bytes()}
	err := ApplyExports(context.Background(), logging.Testing(), storage, store, []ExportSegment{
		{Type: "unknown", Key: "empty-unsupported"},
	})
	require.ErrorContains(t, err, `unsupported export segment type "unknown"`)
}

func TestApplyExportsRejectsIncompleteSequenceCoverage(t *testing.T) {
	t.Parallel()

	key := dal.NewKeyBuilder().
		PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).
		PutUint64(2).
		Build()

	var stream bytes.Buffer
	writer := NewKVStreamWriter(&stream)
	require.NoError(t, writer.WriteHeader())
	require.NoError(t, writer.WriteEntry(key, []byte("value")))
	require.NoError(t, writer.WriteFooter())

	store := newBackupTestStore(t)
	storage := &recordingStorage{manifestBody: stream.Bytes()}
	err := ApplyExports(context.Background(), logging.Testing(), storage, store, []ExportSegment{
		{Type: "log", StartSeq: 1, EndSeq: 2, Key: "missing-start", Size: int64(stream.Len())},
	})
	require.ErrorContains(t, err, "incomplete export segment")
	require.Zero(t, countKeysInSub(t, store, dal.SubHistoryLog),
		"an incomplete segment must not commit its unverified batch")
}

func TestApplyExportsRejectsManifestInconsistentStream(t *testing.T) {
	t.Parallel()

	key := func(seq uint64) []byte {
		return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(seq).Build()
	}
	stream := func(seqs ...uint64) []byte {
		var body bytes.Buffer
		writer := NewKVStreamWriter(&body)
		require.NoError(t, writer.WriteHeader())
		for _, seq := range seqs {
			require.NoError(t, writer.WriteEntry(key(seq), []byte("value")))
		}
		require.NoError(t, writer.WriteFooter())

		return body.Bytes()
	}

	for _, tc := range []struct {
		name string
		body []byte
		size int64
		want string
	}{
		{"early footer", stream(1, 2), int64(len(stream(1, 2, 3))), "incomplete export segment"},
		{"trailing bytes", append(bytes.Clone(stream(1, 2, 3)), 'x'), int64(len(stream(1, 2, 3)) + 1), "trailing bytes"},
		{"wrong manifest size", stream(1, 2, 3), int64(len(stream(1, 2, 3)) + 1), "size mismatch"},
		{"interior gap", stream(1, 3), int64(len(stream(1, 3))), "sequence gap"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			store := newBackupTestStore(t)
			storage := &recordingStorage{manifestBody: tc.body}
			err := ApplyExports(t.Context(), logging.Testing(), storage, store, []ExportSegment{
				{Type: "log", StartSeq: 1, EndSeq: 3, Key: tc.name, Size: tc.size},
			})
			require.ErrorContains(t, err, tc.want)
			require.Zero(t, countKeysInSub(t, store, dal.SubHistoryLog))
		})
	}
}

func TestApplyExportsRejectsAuditItemIndexDisorder(t *testing.T) {
	t.Parallel()

	key := func(index uint32) []byte {
		return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).
			PutUint64(1).PutUint32(index).Build()
	}
	for _, tc := range []struct {
		name    string
		indexes []uint32
	}{
		{"missing first", []uint32{1}},
		{"missing middle", []uint32{0, 2}},
		{"duplicate", []uint32{0, 0}},
		{"reversed", []uint32{1, 0}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var body bytes.Buffer
			writer := NewKVStreamWriter(&body)
			require.NoError(t, writer.WriteHeader())
			for _, index := range tc.indexes {
				require.NoError(t, writer.WriteEntry(key(index), []byte("item")))
			}
			require.NoError(t, writer.WriteFooter())

			store := newBackupTestStore(t)
			err := ApplyExports(t.Context(), logging.Testing(), &recordingStorage{manifestBody: body.Bytes()}, store,
				[]ExportSegment{{Type: "auditItem", StartSeq: 1, EndSeq: 1, Key: tc.name, Size: int64(body.Len())}})
			require.ErrorContains(t, err, "audit item index gap or disorder")
			require.Zero(t, countKeysInSub(t, store, dal.SubHistoryAuditItem))
		})
	}
}

// TestValidateExportKeyRequiresExactKeyShape pins the per-type key length: a key
// carrying trailing bytes still resolves to a valid sequence for prefix scans
// and seqFromKey, so accepting it would let a tampered segment add a second
// record for a sequence already covered by the export range.
func TestValidateExportKeyRequiresExactKeyShape(t *testing.T) {
	t.Parallel()

	seqKey := func(sub byte) []byte {
		return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, sub).PutUint64(1).Build()
	}

	for _, tc := range []struct {
		segType string
		valid   []byte
	}{
		{segType: "log", valid: seqKey(dal.SubHistoryLog)},
		{segType: "audit", valid: seqKey(dal.SubHistoryAudit)},
		{segType: "appliedProposal", valid: seqKey(dal.SubHistoryAppliedProposal)},
		{
			segType: "auditItem",
			valid:   dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryAuditItem).PutUint64(1).PutUint32(0).Build(),
		},
	} {
		t.Run(tc.segType, func(t *testing.T) {
			t.Parallel()

			seg := ExportSegment{Type: tc.segType, StartSeq: 1, EndSeq: 1}

			require.NoError(t, validateExportKey(seg, tc.valid))

			trailing := append(bytes.Clone(tc.valid), 0x00)
			require.ErrorContains(t, validateExportKey(seg, trailing), "is not valid for export segment type")

			truncated := tc.valid[:len(tc.valid)-1]
			require.ErrorContains(t, validateExportKey(seg, truncated), "is not valid for export segment type")
		})
	}
}

// TestApplyExportsRejectsKeyWithTrailingBytes exercises the same rejection
// through the restore path and proves nothing is written to the staged store.
func TestApplyExportsRejectsKeyWithTrailingBytes(t *testing.T) {
	t.Parallel()

	tampered := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneHistory, dal.SubHistoryLog).PutUint64(1).Build()
	tampered = append(tampered, 'x')

	var stream bytes.Buffer
	writer := NewKVStreamWriter(&stream)
	require.NoError(t, writer.WriteHeader())
	require.NoError(t, writer.WriteEntry(tampered, []byte("value")))
	require.NoError(t, writer.WriteFooter())

	store := newBackupTestStore(t)
	storage := &recordingStorage{manifestBody: stream.Bytes()}
	err := ApplyExports(context.Background(), logging.Testing(), storage, store, []ExportSegment{
		{Type: "log", StartSeq: 1, EndSeq: 1, Key: "tampered", Size: int64(stream.Len())},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid key")
	require.Zero(t, countKeysInSub(t, store, dal.SubHistoryLog), "tampered entry must not reach the store")
}

func countKeysInSub(t *testing.T, store *dal.Store, sub byte) int {
	t.Helper()

	handle, err := store.NewDirectReadHandle()
	require.NoError(t, err)

	defer func() { _ = handle.Close() }()

	iter, err := dal.NewBoundedIter(handle,
		[]byte{dal.ZoneHistory, sub},
		[]byte{dal.ZoneHistory, sub + 1},
	)
	require.NoError(t, err)

	defer func() { _ = iter.Close() }()

	count := 0
	for iter.First(); iter.Valid(); iter.Next() {
		count++
	}

	require.NoError(t, iter.Error())

	return count
}
