package signing

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"strconv"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/internal/proto/signaturepb"
)

func TestHTTPAuthorityNormalization(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct{ scheme, authority, want string }{
		{"http", "LEDGER.Example:80", "ledger.example"},
		{"https", "LEDGER.Example:443", "ledger.example"},
		{"https", "LEDGER.Example:80", "ledger.example:80"},
		{"http", "[::1]:80", "[::1]"},
		{"https", "[::1]:8443", "[::1]:8443"},
	} {
		t.Run(tc.scheme+tc.authority, func(t *testing.T) {
			t.Parallel()
			got, err := httpAuthority(tc.scheme, tc.authority)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

func TestHTTPProofSizeIsRequestBounded(t *testing.T) {
	t.Parallel()
	_, privateKey := generateTestKeypair(t)
	proof := &signaturepb.SignedHTTPRequest{Method: "POST", Authority: "ledger.example", RequestTarget: "/v3/main/transactions", ContentType: "application/json"}
	for _, size := range []int{128, 4096, 64 << 10, 4 << 20} {
		proof.Body = bytes.Repeat([]byte("x"), size)
		require.NoError(t, SignHTTP(proof, "ops", privateKey))
		// Only one copy of the HTTP body enters the proof; metadata/header
		// overhead remains constant-size, even at the largest admitted body.
		require.Less(t, proof.SizeVT(), size+1024)
	}
}

func BenchmarkVerifyHTTP(b *testing.B) {
	publicKey, privateKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		b.Fatal(err)
	}
	for _, size := range []int{128, 4096, 64 << 10, 1 << 20} {
		b.Run(strconv.Itoa(size), func(b *testing.B) {
			proof := &signaturepb.SignedHTTPRequest{Method: "POST", Authority: "ledger.example", RequestTarget: "/v3/main/transactions", ContentType: "application/json", Body: bytes.Repeat([]byte("x"), size)}
			if err := SignHTTP(proof, "ops", privateKey); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			b.SetBytes(int64(size))
			b.ResetTimer()
			for b.Loop() {
				if err := VerifyHTTP(proof, publicKey); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func TestVerifyHTTPIndependentSignatureBase(t *testing.T) {
	t.Parallel()
	pub, priv := generateTestKeypair(t)
	// Construct the RFC 9421 signature base independently of the signer. The
	// digest is the published SHA-256 value for the empty message. In particular,
	// @query includes '?' even when the request has no query (RFC 9421 2.2.7).
	proof := &signaturepb.SignedHTTPRequest{
		Method: "DELETE", Authority: "ledger.example",
		Scheme:        "https",
		RequestTarget: "/v3/book/transactions/42/metadata/reviewed",
		ContentType:   "application/json", IdempotencyKey: "delete-42",
		ContentDigest:  "sha-256=:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=:",
		SignatureInput: `sig1=("@method" "@authority" "@path" "@query" "content-type" "content-digest" "idempotency-key");keyid="ops";alg="ed25519"`,
	}
	base := "\"@method\": DELETE\n" +
		"\"@authority\": ledger.example\n" +
		"\"@path\": /v3/book/transactions/42/metadata/reviewed\n" +
		"\"@query\": ?\n" +
		"\"content-type\": application/json\n" +
		"\"content-digest\": sha-256=:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=:\n" +
		"\"idempotency-key\": delete-42\n" +
		`"@signature-params": ("@method" "@authority" "@path" "@query" "content-type" "content-digest" "idempotency-key");keyid="ops";alg="ed25519"`
	proof.Signature = "sig1=:" + base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(base))) + ":"
	require.NoError(t, VerifyHTTP(proof, pub))
	keyID, err := HTTPKeyID(proof)
	require.NoError(t, err)
	require.Equal(t, "ops", keyID)
	for _, tc := range []struct {
		name   string
		mutate func(*signaturepb.SignedHTTPRequest)
	}{
		{"method", func(p *signaturepb.SignedHTTPRequest) { p.Method = "POST" }},
		{"authority", func(p *signaturepb.SignedHTTPRequest) { p.Authority = "other.example" }},
		{"path", func(p *signaturepb.SignedHTTPRequest) {
			p.RequestTarget = "/v3/other/transactions/42/metadata/reviewed"
		}},
		{"query", func(p *signaturepb.SignedHTTPRequest) { p.RequestTarget += "?atomic=true" }},
		{"content type", func(p *signaturepb.SignedHTTPRequest) { p.ContentType += "; charset=utf-8" }},
		{"digest", func(p *signaturepb.SignedHTTPRequest) { p.ContentDigest = "sha-256=:AAAA:" }},
		{"body", func(p *signaturepb.SignedHTTPRequest) { p.Body = []byte(" ") }},
		{"idempotency", func(p *signaturepb.SignedHTTPRequest) { p.IdempotencyKey = "other" }},
		{"key ID", func(p *signaturepb.SignedHTTPRequest) {
			p.SignatureInput = strings.ReplaceAll(p.GetSignatureInput(), `keyid="ops"`, `keyid="other"`)
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := proof.CloneVT()
			tc.mutate(changed)
			require.ErrorIs(t, VerifyHTTP(changed, pub), ErrInvalidSignature)
		})
	}
}

func TestHTTPSignatureProfileRejectsIncompleteOrAmbiguousEvidence(t *testing.T) {
	t.Parallel()
	pub, priv := generateTestKeypair(t)
	proof := &signaturepb.SignedHTTPRequest{Method: "POST", Authority: "ledger.example", RequestTarget: "/v3/book/transactions", ContentType: "application/json", Body: []byte(`{"postings":[]}`)}
	require.NoError(t, SignHTTP(proof, "ops", priv))
	require.NoError(t, VerifyHTTP(proof, pub))
	for _, tc := range []struct {
		name   string
		mutate func(*signaturepb.SignedHTTPRequest)
	}{
		{"missing component", func(p *signaturepb.SignedHTTPRequest) {
			p.SignatureInput = strings.Replace(p.GetSignatureInput(), ` "idempotency-key"`, "", 1)
		}},
		{"duplicate component", func(p *signaturepb.SignedHTTPRequest) {
			p.SignatureInput = strings.Replace(p.GetSignatureInput(), `"@path"`, `"@method"`, 1)
		}},
		{"unknown component", func(p *signaturepb.SignedHTTPRequest) {
			p.SignatureInput = strings.Replace(p.GetSignatureInput(), `"@path"`, `"authorization"`, 1)
		}},
		{"duplicate key ID", func(p *signaturepb.SignedHTTPRequest) { p.SignatureInput += `;keyid="other"` }},
		{"unknown parameter", func(p *signaturepb.SignedHTTPRequest) { p.SignatureInput += `;nonce="unknown"` }},
		{"other algorithm", func(p *signaturepb.SignedHTTPRequest) {
			p.SignatureInput = strings.ReplaceAll(p.GetSignatureInput(), "ed25519", "hmac-sha256")
		}},
		{"second signature", func(p *signaturepb.SignedHTTPRequest) { p.Signature += ", sig2=:AA==:" }},
		{"second input", func(p *signaturepb.SignedHTTPRequest) { p.SignatureInput += ", sig2=()" }},
		{"second digest", func(p *signaturepb.SignedHTTPRequest) { p.ContentDigest += ", sha-256=:AA==:" }},
		{"wrong digest for body", func(p *signaturepb.SignedHTTPRequest) {
			h := sha256.Sum256([]byte("different"))
			p.ContentDigest = "sha-256=:" + base64.StdEncoding.EncodeToString(h[:]) + ":"
		}},
		{"header injection", func(p *signaturepb.SignedHTTPRequest) { p.Authority += "\n\"@path\": /evil" }},
		{"absolute target", func(p *signaturepb.SignedHTTPRequest) {
			p.RequestTarget = "https://ledger.example/v3/book/transactions"
		}},
		{"oversized proof", func(p *signaturepb.SignedHTTPRequest) { p.Body = make([]byte, (4<<20)+1) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			changed := proof.CloneVT()
			tc.mutate(changed)
			require.ErrorIs(t, VerifyHTTP(changed, pub), ErrInvalidSignature)
		})
	}
	_, other := generateTestKeypair(t)
	require.ErrorIs(t, VerifyHTTP(proof, other.Public().(ed25519.PublicKey)), ErrInvalidSignature)
	require.ErrorIs(t, VerifyHTTP(nil, pub), ErrMissingSignature)
}
