package snapshotpb

import (
	"testing"
)

// FuzzPrepareSnapshotResponseUnmarshalVT fuzzes the binary protobuf decoder for
// PrepareSnapshotResponse messages, which carry manifests from the leader.
func FuzzPrepareSnapshotResponseUnmarshalVT(f *testing.F) {
	valid := &PrepareSnapshotResponse{
		SessionId: "abc123",
		Manifest: &SnapshotManifest{
			Files: []*FileEntry{
				{Path: "MANIFEST-000001", Size: 1024, Sha256: "abcdef0123456789"},
			},
		},
	}
	if data, err := valid.MarshalVT(); err == nil {
		f.Add(data)
	}

	empty := &PrepareSnapshotResponse{}
	if data, err := empty.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg PrepareSnapshotResponse
		_ = msg.UnmarshalVT(data)
	})
}

// FuzzFetchFileResponseUnmarshalVT fuzzes the binary protobuf decoder for
// FetchFileResponse messages, which carry file chunks during streaming.
func FuzzFetchFileResponseUnmarshalVT(f *testing.F) {
	chunk := &FetchFileResponse{
		Data: []byte("pebble-sst-data"),
		Eof:  false,
	}
	if data, err := chunk.MarshalVT(); err == nil {
		f.Add(data)
	}

	eofChunk := &FetchFileResponse{Eof: true}
	if data, err := eofChunk.MarshalVT(); err == nil {
		f.Add(data)
	}

	empty := &FetchFileResponse{}
	if data, err := empty.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F}) // varint overflow

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg FetchFileResponse
		_ = msg.UnmarshalVT(data)
	})
}
