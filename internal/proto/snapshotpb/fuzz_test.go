package snapshotpb

import (
	"testing"
)

// FuzzFetchSnapshotResponseUnmarshalVT fuzzes the binary protobuf decoder for
// snapshot download responses. These carry large binary payloads (Pebble DB chunks)
// streamed from peer nodes during Raft snapshot restore.
func FuzzFetchSnapshotResponseUnmarshalVT(f *testing.F) {
	valid := &FetchSnapshotResponse{
		Header: true,
		Data:   []byte("pebble-data-chunk"),
	}
	if data, err := valid.MarshalVT(); err == nil {
		f.Add(data)
	}

	empty := &FetchSnapshotResponse{}
	if data, err := empty.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{0x0A, 0xFF, 0xFF, 0xFF, 0xFF, 0x0F}) // varint overflow

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg FetchSnapshotResponse
		_ = msg.UnmarshalVT(data)
	})
}
