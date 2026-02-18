package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/formancehq/ledger-v3-poc/internal/proto/servicepb"
	"github.com/formancehq/ledger-v3-poc/internal/proto/signaturepb"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
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
	require.NotNil(t, req.Signature)
	require.Equal(t, "key-1", req.Signature.KeyId)
	require.Len(t, req.Signature.Signature, ed25519.SignatureSize)
	require.NotEmpty(t, req.Signature.SignedPayload)

	// Verify succeeds with correct key
	err = Verify(req.Signature, pub)
	require.NoError(t, err)

	// Extract request
	extracted, err := ExtractRequest(req.Signature)
	require.NoError(t, err)
	require.Equal(t, "test-key", extracted.IdempotencyKey)
	require.Equal(t, "my-ledger", extracted.GetCreateLedger().Name)
	// Extracted request should have no signature
	require.Nil(t, extracted.Signature)
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

	err = Verify(req.Signature, otherPub)
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

	err = Verify(req.Signature, pub)
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

	err = Verify(req.Signature, pub)
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
	require.Equal(t, "my-key", sig.KeyId)
	require.Equal(t, payload, sig.SignedPayload)

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

	extracted, err := ExtractRequest(original.Signature)
	require.NoError(t, err)
	require.Equal(t, "idem-123", extracted.IdempotencyKey)
	require.Equal(t, "old-ledger", extracted.GetDeleteLedger().Name)
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
	require.Equal(t, "key-abc", req.IdempotencyKey)
	require.Equal(t, "test", req.GetCreateLedger().Name)

	// The signed_payload should encode the request without signature
	inner := &servicepb.Request{}
	err = proto.Unmarshal(req.Signature.SignedPayload, inner)
	require.NoError(t, err)
	require.Nil(t, inner.Signature)
	require.Equal(t, "key-abc", inner.IdempotencyKey)
}
