package signing

import (
	"crypto/ed25519"
	"encoding/base64"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestHTTPProfileCanonicalizesStructuredFieldsOrderingAndWhitespace(t *testing.T) {
	t.Parallel()
	publicKey, privateKey := generateTestKeypair(t)
	proof := &signaturepb.SignedHTTPRequest{
		Method: "DELETE", Scheme: "https", Authority: "LEDGER.Example:443",
		RequestTarget: "/v3/main/transactions/42/metadata/key",
		ContentType:   "application/json", ContentDigest: "sha-256=:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=:",
		SignatureInput: `sig1=( "content-type"   "@query" "@path" "@method" "content-digest" "@authority" "idempotency-key" ); alg="ed25519";keyid="ops"`,
	}
	// Parameter and component ordering are significant in the base; accepted
	// Structured Fields whitespace is serialized canonically (RFC 9421 2.3).
	base := "\"content-type\": application/json\n" +
		"\"@query\": ?\n" +
		"\"@path\": /v3/main/transactions/42/metadata/key\n" +
		"\"@method\": DELETE\n" +
		"\"content-digest\": sha-256=:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=:\n" +
		"\"@authority\": ledger.example\n" +
		"\"idempotency-key\": \n" +
		`"@signature-params": ("content-type" "@query" "@path" "@method" "content-digest" "@authority" "idempotency-key");alg="ed25519";keyid="ops"`
	proof.Signature = "sig1=:" + base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(base))) + ":"
	require.NoError(t, VerifyHTTP(proof, publicKey))

	// A signature valid over a differently-cased base64 digest must not bypass
	// the body digest check: base64 is case-sensitive, unlike HTTP field names.
	proof.ContentDigest = strings.Replace(proof.GetContentDigest(), "47DE", "47dE", 1)
	base = strings.Replace(base, "47DE", "47dE", 1)
	proof.Signature = "sig1=:" + base64.StdEncoding.EncodeToString(ed25519.Sign(privateKey, []byte(base))) + ":"
	require.ErrorIs(t, VerifyHTTP(proof, publicKey), ErrInvalidSignature)
}

func TestHTTPProfileRequiresIndividualComponentNames(t *testing.T) {
	t.Parallel()
	proof := &signaturepb.SignedHTTPRequest{
		SignatureInput: `sig1=("@method" "@authority" "@method\" \"@authority" "@query" "content-type" "content-digest" "idempotency-key");keyid="ops";alg="ed25519"`,
	}
	_, err := HTTPKeyID(proof)
	require.ErrorIs(t, err, ErrInvalidSignature)
}

func TestHTTPByteSequenceTruncationRejectsWithoutPanic(t *testing.T) {
	t.Parallel()
	publicKey, privateKey := generateTestKeypair(t)
	proof := &signaturepb.SignedHTTPRequest{Method: "POST", Authority: "ledger.example", RequestTarget: "/v3/main/transactions", ContentType: "application/json", Body: []byte(`{}`)}
	require.NoError(t, SignHTTP(proof, "ops", privateKey))
	for _, tc := range []struct {
		name   string
		mutate func(*signaturepb.SignedHTTPRequest)
	}{
		{"truncated signature", func(p *signaturepb.SignedHTTPRequest) { p.Signature = "sig1=:" }},
		{"truncated digest", func(p *signaturepb.SignedHTTPRequest) { p.ContentDigest = "sha-256=:" }},
		{"line break in signature", func(p *signaturepb.SignedHTTPRequest) {
			p.Signature = strings.Replace(p.GetSignature(), "sig1=:", "sig1=:\n", 1)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := proof.CloneVT()
			tc.mutate(changed)
			require.NotPanics(t, func() { require.ErrorIs(t, VerifyHTTP(changed, publicKey), ErrInvalidSignature) })
		})
	}
}
