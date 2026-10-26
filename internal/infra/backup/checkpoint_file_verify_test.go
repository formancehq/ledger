package backup

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func checkpointFileFor(data []byte) CheckpointFile {
	sum := sha256.Sum256(data)

	return CheckpointFile{
		Size: int64(len(data)),
		Key:  CheckpointFileKey("bucket", "000002.log", hex.EncodeToString(sum[:])),
	}
}

func TestCheckpointFileCopyVerifiedAcceptsIntactObject(t *testing.T) {
	t.Parallel()

	data := []byte("checkpoint bytes")
	var dst bytes.Buffer

	n, err := checkpointFileFor(data).CopyVerified(&dst, bytes.NewReader(data))
	require.NoError(t, err)
	require.EqualValues(t, len(data), n)
	require.Equal(t, data, dst.Bytes())
}

func TestCheckpointFileCopyVerifiedRejectsMismatch(t *testing.T) {
	t.Parallel()

	data := []byte("checkpoint bytes")
	cf := checkpointFileFor(data)

	cases := []struct {
		name    string
		entry   CheckpointFile
		served  []byte
		wantErr string
	}{
		{
			name:    "same size, different bytes",
			entry:   cf,
			served:  []byte("CHECKPOINT BYTES"),
			wantErr: "sha256 mismatch",
		},
		{
			name:    "truncated object",
			entry:   cf,
			served:  data[:4],
			wantErr: "size mismatch: got 4 bytes, want 16",
		},
		{
			name:    "oversized object",
			entry:   cf,
			served:  append(bytes.Clone(data), 'x'),
			wantErr: "size mismatch: object is larger than 16 bytes",
		},
		{
			name:    "key without content hash",
			entry:   CheckpointFile{Size: cf.Size, Key: "bucket/data/000002.log"},
			served:  data,
			wantErr: "carries no sha256",
		},
		{
			name:    "negative size",
			entry:   CheckpointFile{Size: -1, Key: cf.Key},
			served:  data,
			wantErr: "negative size",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := tc.entry.CopyVerified(&bytes.Buffer{}, bytes.NewReader(tc.served))
			require.ErrorIs(t, err, ErrCheckpointFileCorrupted)
			require.ErrorContains(t, err, tc.wantErr)
		})
	}
}

// TestCheckpointFileCopyVerifiedBoundsOversizedRead pins that an oversized
// object is not copied in full: at most Size+1 bytes reach dst.
func TestCheckpointFileCopyVerifiedBoundsOversizedRead(t *testing.T) {
	t.Parallel()

	data := []byte("checkpoint bytes")
	var dst bytes.Buffer

	_, err := checkpointFileFor(data).CopyVerified(&dst, strings.NewReader(strings.Repeat("x", 1<<20)))
	require.ErrorIs(t, err, ErrCheckpointFileCorrupted)
	require.Equal(t, len(data)+1, dst.Len())
}
