package signaturepb

import (
	"testing"
)

// FuzzRequestSignatureUnmarshalVT fuzzes the binary protobuf decoder for Ed25519
// request signatures. These are embedded in every Raft Order and verified on
// every request. Malformed signatures must not cause panics or bypass validation.
func FuzzRequestSignatureUnmarshalVT(f *testing.F) {
	valid := &RequestSignature{
		KeyId:         "key-1",
		Signature:     []byte("fake-ed25519-sig-64-bytes-padding-padding-padding-padding-padding"),
		SignedPayload: []byte("payload-bytes"),
	}
	if data, err := valid.MarshalVT(); err == nil {
		f.Add(data)
	}

	empty := &RequestSignature{}
	if data, err := empty.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg RequestSignature
		_ = msg.UnmarshalVT(data)
	})
}

// FuzzResponseSignatureUnmarshalVT fuzzes the binary protobuf decoder for response signatures.
func FuzzResponseSignatureUnmarshalVT(f *testing.F) {
	valid := &ResponseSignature{
		KeyId:         "key-1",
		Signature:     []byte("fake-sig"),
		SignedPayload: []byte("response-payload"),
	}
	if data, err := valid.MarshalVT(); err == nil {
		f.Add(data)
	}

	f.Add([]byte{})
	f.Add([]byte{0xFF})

	f.Fuzz(func(t *testing.T, data []byte) {
		var msg ResponseSignature
		_ = msg.UnmarshalVT(data)
	})
}
