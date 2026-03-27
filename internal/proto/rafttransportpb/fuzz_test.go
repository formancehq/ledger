package rafttransportpb

import (
	"testing"
)

// FuzzRaftRequestBatchUnmarshalVT fuzzes the binary protobuf decoder for Raft request batches.
// These messages are deserialized from untrusted peer-to-peer network streams
// (bidirectional gRPC streaming). Malformed payloads must not cause panics,
// out-of-bounds reads, or infinite loops.
func FuzzRaftRequestBatchUnmarshalVT(f *testing.F) {
	// Seed with valid encoded messages.
	empty := &RaftRequestBatch{}
	if data, err := empty.MarshalVT(); err == nil {
		f.Add(data)
	}

	batch := &RaftRequestBatch{
		Messages: []*RaftRequestMessage{
			{Id: 1, Message: []byte("raft-msg")},
		},
	}
	if data, err := batch.MarshalVT(); err == nil {
		f.Add(data)
	}

	// Edge cases: empty, truncated, garbage.
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF})
	f.Add([]byte{0x0A, 0x80, 0x80, 0x80, 0x80, 0x00}) // varint with excessive length

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg RaftRequestBatch
		_ = msg.UnmarshalVT(data)
	})
}

// FuzzSendMessageRequestUnmarshalVT fuzzes the oneof dispatch for incoming Raft messages.
// SendMessageRequest wraps either a RaftRequestBatch or a PingMessage.
func FuzzSendMessageRequestUnmarshalVT(f *testing.F) {
	// Raft variant.
	req := &SendMessageRequest{
		Message: &SendMessageRequest_Raft{
			Raft: &RaftRequestBatch{},
		},
	}
	if data, err := req.MarshalVT(); err == nil {
		f.Add(data)
	}

	// Ping variant.
	ping := &SendMessageRequest{
		Message: &SendMessageRequest_Ping{
			Ping: &PingMessage{SeqId: 1},
		},
	}
	if data, err := ping.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0x0A, 0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg SendMessageRequest
		_ = msg.UnmarshalVT(data)
	})
}
