package backup

import (
	"bytes"
	"context"
	"testing"

	logging "github.com/formancehq/go-libs/v5/pkg/observe/log"
	"github.com/stretchr/testify/require"
)

func TestApplyExportsRejectsKeyOutsideSegmentKeyspace(t *testing.T) {
	var stream bytes.Buffer
	writer := NewKVStreamWriter(&stream)
	require.NoError(t, writer.WriteHeader())
	require.NoError(t, writer.WriteEntry([]byte{0xff, 0x01, 'x'}, []byte("value")))
	require.NoError(t, writer.WriteFooter())

	store := newBackupTestStore(t)
	storage := &recordingStorage{manifestBody: stream.Bytes()}
	err := ApplyExports(context.Background(), testLogger(), storage, store, []ExportSegment{
		{Type: "log", StartSeq: 1, EndSeq: 1, Key: "invalid"},
	})
	require.Error(t, err)
	require.ErrorContains(t, err, "invalid key")
}

func testLogger() logging.Logger { return logging.Testing() }
