package signing

import (
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func generateTestKeypair(t *testing.T) (ed25519.PublicKey, ed25519.PrivateKey) {
	t.Helper()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	require.NoError(t, err)

	return pub, priv
}

func createLedgerBatch(idempotencyKey, ledgerName string) *servicepb.ApplyBatch {
	return &servicepb.ApplyBatch{
		IdempotencyKey: idempotencyKey,
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_CreateLedger{
					CreateLedger: &servicepb.CreateLedgerRequest{Name: ledgerName},
				},
			},
		},
	}
}

func TestSignAndVerifyRoundTrip(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	batch := createLedgerBatch("test-key", "my-ledger")

	sr, err := Sign(batch, "key-1", priv)
	require.NoError(t, err)
	require.NotNil(t, sr)
	require.Equal(t, "key-1", sr.GetKeyId())
	require.Len(t, sr.GetSignature(), ed25519.SignatureSize)
	require.NotEmpty(t, sr.GetPayload())

	// Verify succeeds with correct key
	err = Verify(sr, pub)
	require.NoError(t, err)

	// Extract batch
	extracted, err := ExtractBatch(sr)
	require.NoError(t, err)
	require.Equal(t, "test-key", extracted.GetIdempotencyKey())
	require.Len(t, extracted.GetRequests(), 1)
	require.Equal(t, "my-ledger", extracted.GetRequests()[0].GetCreateLedger().GetName())
}

func TestVerifyWrongKey(t *testing.T) {
	t.Parallel()

	_, priv := generateTestKeypair(t)
	otherPub, _ := generateTestKeypair(t)

	batch := createLedgerBatch("", "ledger")

	sr, err := Sign(batch, "key-1", priv)
	require.NoError(t, err)

	err = Verify(sr, otherPub)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestVerifyModifiedPayload(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	batch := createLedgerBatch("", "ledger")

	sr, err := Sign(batch, "key-1", priv)
	require.NoError(t, err)

	// Tamper with payload after signing
	sr.Payload[0] ^= 0xFF

	err = Verify(sr, pub)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestVerifyModifiedSignature(t *testing.T) {
	t.Parallel()

	pub, priv := generateTestKeypair(t)

	batch := createLedgerBatch("", "ledger")

	sr, err := Sign(batch, "key-1", priv)
	require.NoError(t, err)

	// Tamper with signature
	sr.Signature[0] ^= 0xFF

	err = Verify(sr, pub)
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

	sr := &signaturepb.SignedApplyBatch{
		KeyId:     "key-1",
		Signature: make([]byte, ed25519.SignatureSize),
		Payload:   nil,
	}

	err := Verify(sr, pub)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestExtractBatchPreservesContent(t *testing.T) {
	t.Parallel()

	_, priv := generateTestKeypair(t)

	original := &servicepb.ApplyBatch{
		IdempotencyKey: "idem-123",
		Requests: []*servicepb.Request{
			{
				Type: &servicepb.Request_DeleteLedger{
					DeleteLedger: &servicepb.DeleteLedgerRequest{Name: "old-ledger"},
				},
			},
		},
	}

	sr, err := Sign(original, "key-1", priv)
	require.NoError(t, err)

	extracted, err := ExtractBatch(sr)
	require.NoError(t, err)
	require.Equal(t, "idem-123", extracted.GetIdempotencyKey())
	require.Len(t, extracted.GetRequests(), 1)
	require.Equal(t, "old-ledger", extracted.GetRequests()[0].GetDeleteLedger().GetName())
}

func TestVerifyInvalidSignatureLength(t *testing.T) {
	t.Parallel()

	pub, _ := generateTestKeypair(t)

	sr := &signaturepb.SignedApplyBatch{
		KeyId:     "key-1",
		Signature: []byte("too-short"),
		Payload:   []byte("payload"),
	}

	err := Verify(sr, pub)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestExtractBatchNil(t *testing.T) {
	t.Parallel()

	_, err := ExtractBatch(nil)
	require.ErrorIs(t, err, ErrMissingSignature)
}

func TestExtractBatchEmptyPayload(t *testing.T) {
	t.Parallel()

	sr := &signaturepb.SignedApplyBatch{
		KeyId:   "key-1",
		Payload: nil,
	}

	_, err := ExtractBatch(sr)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestExtractBatchInvalidPayload(t *testing.T) {
	t.Parallel()

	sr := &signaturepb.SignedApplyBatch{
		KeyId:   "key-1",
		Payload: []byte("not-valid-proto"),
	}

	_, err := ExtractBatch(sr)
	require.Error(t, err)
}

func TestSignDoesNotMutateOriginal(t *testing.T) {
	t.Parallel()

	_, priv := generateTestKeypair(t)

	batch := createLedgerBatch("key-abc", "test")

	_, err := Sign(batch, "key-1", priv)
	require.NoError(t, err)

	// Original batch fields are unchanged
	require.Equal(t, "key-abc", batch.GetIdempotencyKey())
	require.Len(t, batch.GetRequests(), 1)
	require.Equal(t, "test", batch.GetRequests()[0].GetCreateLedger().GetName())
}
