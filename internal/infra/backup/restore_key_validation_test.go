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
		{Type: "log", StartSeq: 1, EndSeq: 1, Key: "invalid"},
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

// TestValidateExportKeyRequiresExactKeyShape pins the per-type key length: a key
// carrying trailing bytes still resolves to a valid sequence for prefix scans
// and seqFromKey, so accepting it would let a tampered segment add a second
// record for a sequence already covered by the export range.
func TestValidateExportKeyRequiresExactKeyShape(t *testing.T) {
	t.Parallel()

	seqKey := func(sub byte) []byte {
		return dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, sub).PutUint64(1).Build()
	}

	for _, tc := range []struct {
		segType string
		valid   []byte
	}{
		{segType: "log", valid: seqKey(dal.SubColdLog)},
		{segType: "audit", valid: seqKey(dal.SubColdAudit)},
		{segType: "appliedProposal", valid: seqKey(dal.SubColdAppliedProposal)},
		{
			segType: "auditItem",
			valid:   dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdAuditItem).PutUint64(1).PutUint32(0).Build(),
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

	tampered := dal.NewKeyBuilder().PutZonePrefix(dal.ZoneCold, dal.SubColdLog).PutUint64(1).Build()
	tampered = append(tampered, 'x')

	var stream bytes.Buffer
	writer := NewKVStreamWriter(&stream)
	require.NoError(t, writer.WriteHeader())
	require.NoError(t, writer.WriteEntry(tampered, []byte("value")))
	require.NoError(t, writer.WriteFooter())

	store := newBackupTestStore(t)
	storage := &recordingStorage{manifestBody: stream.Bytes()}
	err := ApplyExports(context.Background(), logging.Testing(), storage, store, []ExportSegment{
		{Type: "log", StartSeq: 1, EndSeq: 1, Key: "tampered"},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid key")
	require.Zero(t, countKeysInSub(t, store, dal.SubColdLog), "tampered entry must not reach the store")
}
