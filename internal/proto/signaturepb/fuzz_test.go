package signaturepb

import (
	"testing"
)

// FuzzSignedApplyBatchUnmarshalVT fuzzes the binary protobuf decoder for Ed25519
// batch signatures. These are embedded in every Raft Proposal and verified on
// every batch. Malformed signatures must not cause panics or bypass validation.
func FuzzSignedApplyBatchUnmarshalVT(f *testing.F) {
	valid := &SignedApplyBatch{
		KeyId:     "key-1",
		Signature: []byte("fake-ed25519-sig-64-bytes-padding-padding-padding-padding-padding"),
		Payload:   []byte("payload-bytes"),
	}
	if data, err := valid.MarshalVT(); err == nil {
		f.Add(data)
	}

	empty := &SignedApplyBatch{}
	if data, err := empty.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg SignedApplyBatch
		_ = msg.UnmarshalVT(data)
	})
}

// FuzzSignedLogUnmarshalVT fuzzes the binary protobuf decoder for response signatures.
func FuzzSignedLogUnmarshalVT(f *testing.F) {
	valid := &SignedLog{
		KeyId:     "key-1",
		Signature: []byte("fake-sig"),
		Payload:   []byte("response-payload"),
	}
	if data, err := valid.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg SignedLog
		_ = msg.UnmarshalVT(data)
	})
}
