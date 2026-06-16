package signaturepb

import (
	"testing"
)

// FuzzSignedRequestUnmarshalVT fuzzes the binary protobuf decoder for Ed25519
// request signatures. These are embedded in every Raft Order and verified on
// every request. Malformed signatures must not cause panics or bypass validation.
func FuzzSignedRequestUnmarshalVT(f *testing.F) {
	valid := &SignedRequest{
		KeyId:     "key-1",
		Signature: []byte("fake-ed25519-sig-64-bytes-padding-padding-padding-padding-padding"),
		Payload:   []byte("payload-bytes"),
	}
	if data, err := valid.MarshalVT(); err == nil {
		f.Add(data)
	}

	empty := &SignedRequest{}
	if data, err := empty.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg SignedRequest
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
