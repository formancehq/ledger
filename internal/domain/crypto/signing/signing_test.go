package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func generateTestKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	return pub, priv
}

func TestSignAndVerifyRoundTrip(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	req := &servicepb.Request{
		IdempotencyKey: "test-key",
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{
				Name: "my-ledger",
			},
		},
	}

	err := Sign(req, "key-1", priv)
	require.NoError(t, err)
	require.NotNil(t, req.GetSignature())
	require.Equal(t, "key-1", req.GetSignature().GetKeyId())
	require.Len(t, req.GetSignature().GetSignature(), ed25519.SignatureSize)
	require.NotEmpty(t, req.GetSignature().GetSignedPayload())

	// Verify succeeds with correct key
	err = Verify(req.GetSignature(), pub)
	require.NoError(t, err)

	// Extract request
	extracted, err := ExtractRequest(req.GetSignature())
	require.NoError(t, err)
	require.Equal(t, "test-key", extracted.GetIdempotencyKey())
	require.Equal(t, "my-ledger", extracted.GetCreateLedger().GetName())
	// Extracted request should have no signature
	require.Nil(t, extracted.GetSignature())
}

func TestVerifyWrongKey(t *testing.T) {
	t.Parallel()

	_, priv := generateTestKeypair(t)
	otherPub, _ := generateTestKeypair(t)

	req := &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: "ledger"},
		},
	}

	err := Sign(req, "key-1", priv)
	require.NoError(t, err)

	err = Verify(req.GetSignature(), otherPub)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestVerifyModifiedPayload(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	req := &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: "ledger"},
		},
	}

	err := Sign(req, "key-1", priv)
	require.NoError(t, err)

	// Tamper with signed_payload
	req.Signature.SignedPayload[0] ^= 0xFF

	err = Verify(req.GetSignature(), pub)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestVerifyModifiedSignature(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	req := &servicepb.Request{
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: "ledger"},
		},
	}

	err := Sign(req, "key-1", priv)
	require.NoError(t, err)

	// Tamper with signature
	req.Signature.Signature[0] ^= 0xFF

	err = Verify(req.GetSignature(), pub)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestVerifyNilSignature(t *testing.T) {
	t.Parallel()

	pub, _ := generateTestKeypair(t)

	err := Verify(nil, pub)
	require.ErrorIs(t, err, ErrMissingSignature)
}

func TestVerifyEmptyPayload(t *testing.T) {
	t.Parallel()

	pub, _ := generateTestKeypair(t)

	sig := &signaturepb.RequestSignature{
		KeyId:         "key-1",
		Signature:     make([]byte, ed25519.SignatureSize),
		SignedPayload: nil,
	}

	err := Verify(sig, pub)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestSignPayload(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)
	payload := []byte("hello world")

	sig := SignPayload(payload, "my-key", priv)
	require.Equal(t, "my-key", sig.GetKeyId())
	require.Equal(t, payload, sig.GetSignedPayload())

	err := Verify(sig, pub)
	require.NoError(t, err)
}

func TestExtractRequestPreservesContent(t *testing.T) {
	t.Parallel()

	_, priv := generateTestKeypair(t)

	original := &servicepb.Request{
		IdempotencyKey: "idem-123",
		Type: &servicepb.Request_DeleteLedger{
			DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "old-ledger"},
		},
	}

	err := Sign(original, "key-1", priv)
	require.NoError(t, err)

	extracted, err := ExtractRequest(original.GetSignature())
	require.NoError(t, err)
	require.Equal(t, "idem-123", extracted.GetIdempotencyKey())
	require.Equal(t, "old-ledger", extracted.GetDeleteLedger().GetName())
}

func TestVerifyInvalidSignatureLength(t *testing.T) {
	t.Parallel()

	pub, _ := generateTestKeypair(t)

	sig := &signaturepb.RequestSignature{
		KeyId:         "key-1",
		Signature:     []byte("too-short"),
		SignedPayload: []byte("payload"),
	}

	err := Verify(sig, pub)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestExtractRequestNil(t *testing.T) {
	t.Parallel()

	_, err := ExtractRequest(nil)
	require.ErrorIs(t, err, ErrMissingSignature)
}

func TestExtractRequestEmptyPayload(t *testing.T) {
	t.Parallel()

	sig := &signaturepb.RequestSignature{
		KeyId:         "key-1",
		SignedPayload: nil,
	}

	_, err := ExtractRequest(sig)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestExtractRequestInvalidPayload(t *testing.T) {
	t.Parallel()

	sig := &signaturepb.RequestSignature{
		KeyId:         "key-1",
		SignedPayload: []byte("not-valid-proto"),
	}

	_, err := ExtractRequest(sig)
	require.Error(t, err)
}

func TestSignDoesNotMutateOriginalFields(t *testing.T) {
	t.Parallel()

	_, priv := generateTestKeypair(t)

	req := &servicepb.Request{
		IdempotencyKey: "key-abc",
		Type: &servicepb.Request_CreateLedger{
			CreateLedger: &servicepb.CreateLedgerRequest{Name: "test"},
		},
	}

	err := Sign(req, "key-1", priv)
	require.NoError(t, err)

	// Original request fields should be unchanged
	require.Equal(t, "key-abc", req.GetIdempotencyKey())
	require.Equal(t, "test", req.GetCreateLedger().GetName())

	// The signed_payload should encode the request without signature
	inner := &servicepb.Request{}
	err = proto.Unmarshal(req.GetSignature().GetSignedPayload(), inner)
	require.NoError(t, err)
	require.Nil(t, inner.GetSignature())
	require.Equal(t, "key-abc", inner.GetIdempotencyKey())
}
