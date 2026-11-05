package admission

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/formancehq/ledger/v3/internal/domain/crypto/signing"
	"github.com/formancehq/ledger/v3/internal/proto/servicepb"
	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestResolveBatchHTTPSignatureDerivesAuthenticatedIntent(t *testing.T) {
	t.Parallel()
	adm, _ := createTestAdmission(t, createTestStore(t))
	publicKey, privateKey := generateTestKeyPair(t)
	adm.keyStore.AddPublicKey("ops", publicKey, "")
	adm.sharedState.SetRequireSignatures(true)
	proof := &signaturepb.SignedHTTPRequest{
		Method: "POST", Authority: "ledger.example", RequestTarget: "/v3/main/transactions/42/metadata",
		ContentType: "application/json", IdempotencyKey: "metadata-42", Body: []byte(`{"reviewed":"yes"}`),
	}
	require.NoError(t, signing.SignHTTP(proof, "ops", privateKey))
	request := &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_HttpSigned{HttpSigned: proof}}
	resolved, err := adm.resolveBatch(context.Background(), request)
	require.NoError(t, err)
	require.Equal(t, "metadata-42", resolved.key)
	require.Same(t, proof, resolved.httpSig)
	require.Nil(t, resolved.sig)
	require.Len(t, resolved.requests, 1)
	apply := resolved.requests[0].GetApply()
	require.Equal(t, "main", apply.GetLedger())
	require.Equal(t, uint64(42), apply.GetAction().GetAddMetadata().GetTarget().GetTransactionId())
	require.Equal(t, "yes", apply.GetAction().GetAddMetadata().GetMetadata()["reviewed"].GetStringValue())
	plan, err := adm.buildApplyAdmissionPlan(context.Background(), request)
	require.NoError(t, err)
	require.True(t, proto.Equal(proof, plan.httpSig))

	for _, tc := range []struct {
		name   string
		mutate func(*signaturepb.SignedHTTPRequest)
	}{
		{"body", func(p *signaturepb.SignedHTTPRequest) { p.Body = []byte(`{"reviewed":"no"}`) }},
		{"target", func(p *signaturepb.SignedHTTPRequest) { p.RequestTarget = "/v3/main/transactions/43/metadata" }},
		{"idempotency", func(p *signaturepb.SignedHTTPRequest) { p.IdempotencyKey = "other" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := proof.CloneVT()
			tc.mutate(changed)
			_, err := adm.resolveBatch(context.Background(), &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_HttpSigned{HttpSigned: changed}})
			require.ErrorIs(t, err, signing.ErrInvalidSignature)
		})
	}
}

func TestResolveBatchHTTPSignatureRejectsUnsupportedIntentAndUnknownKey(t *testing.T) {
	t.Parallel()
	adm, _ := createTestAdmission(t, createTestStore(t))
	publicKey, privateKey := generateTestKeyPair(t)
	adm.keyStore.AddPublicKey("ops", publicKey, "")
	proof := &signaturepb.SignedHTTPRequest{
		Method: "POST", Authority: "ledger.example", RequestTarget: "/v3/main/transactions",
		ContentType: "application/json", Body: []byte(`{"reference":"r"}`),
	}
	require.NoError(t, signing.SignHTTP(proof, "unknown", privateKey))
	_, err := adm.resolveBatch(context.Background(), &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_HttpSigned{HttpSigned: proof}})
	require.ErrorIs(t, err, signing.ErrUnknownKeyID)
	// A valid cryptographic signature over an unsupported route cannot become
	// arbitrary gRPC commands: admission derives only the supported HTTP intent.
	proof.RequestTarget = "/v3/main/indexes"
	require.NoError(t, signing.SignHTTP(proof, "ops", privateKey))
	_, err = adm.resolveBatch(context.Background(), &servicepb.ApplyRequest{Variant: &servicepb.ApplyRequest_HttpSigned{HttpSigned: proof}})
	require.ErrorIs(t, err, signing.ErrInvalidSignature)
}
