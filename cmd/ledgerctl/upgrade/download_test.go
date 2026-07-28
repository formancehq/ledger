package upgrade

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExtractBinary(t *testing.T) {
	t.Parallel()

	const binaryContents = "ledgerctl binary"

	tests := []struct {
		name        string
		archiveName string
		binaryName  string
		archive     func(*testing.T, string, string) []byte
	}{
		{
			name:        "tar.gz",
			archiveName: "ledger-v3_linux-amd64.tar.gz",
			binaryName:  "ledgerctl",
			archive:     tarGzArchive,
		},
		{
			name:        "zip",
			archiveName: "ledger-v3_windows-amd64.zip",
			binaryName:  "ledgerctl.exe",
			archive:     zipArchive,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			archive := test.archive(t, "ledger-v3/"+test.binaryName, binaryContents)
			archiveReader := bytes.NewReader(archive)

			extractedPath, err := extractBinary(
				archiveReader,
				int64(len(archive)),
				test.archiveName,
				test.binaryName,
			)
			require.NoError(t, err)
			t.Cleanup(func() {
				require.NoError(t, os.Remove(extractedPath))
			})

			extracted, err := os.ReadFile(extractedPath)
			require.NoError(t, err)
			require.Equal(t, binaryContents, string(extracted))
		})
	}
}

func TestExtractBinaryRejectsUnknownArchive(t *testing.T) {
	t.Parallel()

	archive := bytes.NewReader(nil)
	_, err := extractBinary(archive, 0, "ledger-v3_windows-amd64.rar", "ledgerctl.exe")
	require.EqualError(t, err, `unsupported archive format "ledger-v3_windows-amd64.rar"`)
}

func tarGzArchive(t *testing.T, binaryName, contents string) []byte {
	t.Helper()

	var archive bytes.Buffer

	gz := gzip.NewWriter(&archive)
	tw := tar.NewWriter(gz)

	require.NoError(t, tw.WriteHeader(&tar.Header{
		Name: binaryName,
		Mode: 0o755,
		Size: int64(len(contents)),
	}))
	_, err := tw.Write([]byte(contents))
	require.NoError(t, err)
	require.NoError(t, tw.Close())
	require.NoError(t, gz.Close())

	return archive.Bytes()
}

func zipArchive(t *testing.T, binaryName, contents string) []byte {
	t.Helper()

	var archive bytes.Buffer

	zw := zip.NewWriter(&archive)
	binary, err := zw.Create(binaryName)
	require.NoError(t, err)
	_, err = binary.Write([]byte(contents))
	require.NoError(t, err)
	require.NoError(t, zw.Close())

	return archive.Bytes()
}
