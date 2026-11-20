package signing

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/encoding/protowire"

	"github.com/formancehq/ledger/v3/internal/proto/commonpb"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestResponseReceiptHashesExactClientBytes(t *testing.T) {
	t.Parallel()
	signer := NewResponseSigner(make([]byte, ed25519.SeedSize))
	batch := &servicepb.ApplyBatch{IdempotencyKey: "k"}
	request := &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_Unsigned{Unsigned: batch}}
	// Both byte sequences decode to the same batch. The second repeats field 2.
	first := []byte{0x12, 0x01, 'k'}
	second := protowire.AppendString(protowire.AppendTag(bytes.Clone(first), 2, protowire.BytesType), "k")
	request.ResponseBindingPayload = first
	log := &commonpb.Log{Sequence: 7, Payload: &commonpb.LogPayload{Type: &commonpb.LogPayload_DeletedPreparedQuery{DeletedPreparedQuery: &commonpb.DeletedPreparedQueryLog{Ledger: "test", Name: "old"}}}}
	log.ResponseSignature = signer.SignLog(log)
	signed, err := signer.SignResponseReceipt(request, []*commonpb.Log{log})
	require.NoError(t, err)
	var receipt signaturepb.ResponseReceipt
	require.NoError(t, receipt.UnmarshalVT(signed.GetPayload()))
	hash := sha256.Sum256(first)
	require.Equal(t, hash[:], receipt.GetBatchHash())
	require.NoError(t, VerifyResponseReceipt(request, []*commonpb.Log{log}, signed, signer.PublicKey()))
	request.ResponseBindingPayload = second
	require.ErrorContains(t, VerifyResponseReceipt(request, []*commonpb.Log{log}, signed, signer.PublicKey()), "does not match submitted batch")
	request.ResponseBindingPayload = []byte{0x12, 0x01, 'x'}
	_, err = signer.SignResponseReceipt(request, []*commonpb.Log{log})
	require.ErrorContains(t, err, "response binding payload does not match submitted batch")
}
