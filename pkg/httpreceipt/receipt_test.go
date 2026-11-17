package httpreceipt_test

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/formancehq/ledger/v3/pkg/httpreceipt"
)

func digest(value string) string {
	h := sha256.Sum256([]byte(value))
	return hex.EncodeToString(h[:])
}

func TestReceiptBindsObservedResponse(t *testing.T) {
	t.Parallel()
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	claims := httpreceipt.Claims{Version: httpreceipt.Version, Method: "POST", TargetHash: digest("/ledger/transactions?x=1"), Status: 200, BodyHash: digest("{\"data\":1}\n"), Applies: 1, Logs: 1, SequencesHash: digest("sequence")}
	header, err := httpreceipt.Sign(key, claims)
	require.NoError(t, err)
	actual, err := httpreceipt.Verify(header, key.Public().(ed25519.PublicKey), "POST", "/ledger/transactions?x=1", 200, []byte("{\"data\":1}\n"))
	require.NoError(t, err)
	require.Equal(t, claims, *actual)
}

func TestReceiptRejectsTampering(t *testing.T) {
	t.Parallel()
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	publicKey := key.Public().(ed25519.PublicKey)
	claims := httpreceipt.Claims{Version: "v1", Method: "DELETE", TargetHash: digest("/v3/book/metadata/a"), BodyHash: digest(""), Status: 204, Applies: 1, Logs: 1, Replayed: 1, SequencesHash: digest("sequence")}
	header, err := httpreceipt.Sign(key, claims)
	require.NoError(t, err)
	_, err = httpreceipt.Verify(header, publicKey, "DELETE", "/v3/book/metadata/a", 204, nil)
	require.NoError(t, err)
	for _, test := range []struct {
		name, method, target string
		status               int
		body                 []byte
	}{
		{"method", "POST", "/v3/book/metadata/a", 204, nil},
		{"target", "DELETE", "/v3/book/metadata/b", 204, nil},
		{"query", "DELETE", "/v3/book/metadata/a?x=1", 204, nil},
		{"status", "DELETE", "/v3/book/metadata/a", 200, nil},
		{"body", "DELETE", "/v3/book/metadata/a", 204, []byte(" ")},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := httpreceipt.Verify(header, publicKey, test.method, test.target, test.status, test.body)
			require.ErrorContains(t, err, "does not match")
		})
	}
	for _, bad := range []string{"", strings.Repeat("a", 4097), "!", base64.RawURLEncoding.EncodeToString([]byte("null"))} {
		_, err := httpreceipt.Verify(bad, publicKey, "DELETE", "/v3/book/metadata/a", 204, nil)
		require.Error(t, err)
	}
	for _, badKey := range []ed25519.PublicKey{nil, make([]byte, 31), make([]byte, 32)} {
		_, err := httpreceipt.Verify(header, badKey, "DELETE", "/v3/book/metadata/a", 204, nil)
		require.Error(t, err)
	}
	_, err = httpreceipt.Sign(nil, claims)
	require.Error(t, err)
	raw, err := base64.RawURLEncoding.DecodeString(header)
	require.NoError(t, err)
	var env struct {
		KeyID     string `json:"keyId"`
		Payload   []byte `json:"payload"`
		Signature []byte `json:"signature"`
	}
	require.NoError(t, json.Unmarshal(raw, &env))
	for _, field := range []string{"payload", "signature"} {
		t.Run(field, func(t *testing.T) {
			changed := env
			if field == "payload" {
				changed.Payload = append([]byte(nil), env.Payload...)
				changed.Payload[0] ^= 1
			} else {
				changed.Signature = append([]byte(nil), env.Signature...)
				changed.Signature[0] ^= 1
			}
			encoded, err := json.Marshal(changed)
			require.NoError(t, err)
			_, err = httpreceipt.Verify(base64.RawURLEncoding.EncodeToString(encoded), publicKey, "DELETE", "/v3/book/metadata/a", 204, nil)
			require.ErrorContains(t, err, "signature verification failed")
		})
	}
	// Independently construct the wire envelope, signing original JSON including
	// whitespace. No canonical reserialization is required by the verifier.
	env.Payload = append([]byte("  "), env.Payload...)
	env.KeyID = "untrusted-hint"
	env.Signature = ed25519.Sign(key, append([]byte("formance.http-public-receipt.v1\x00"), env.Payload...))
	encoded, err := json.Marshal(env)
	require.NoError(t, err)
	_, err = httpreceipt.Verify(base64.RawURLEncoding.EncodeToString(encoded), publicKey, "DELETE", "/v3/book/metadata/a", 204, nil)
	require.NoError(t, err)
	env.Signature = ed25519.Sign(key, env.Payload)
	encoded, err = json.Marshal(env)
	require.NoError(t, err)
	_, err = httpreceipt.Verify(base64.RawURLEncoding.EncodeToString(encoded), publicKey, "DELETE", "/v3/book/metadata/a", 204, nil)
	require.ErrorContains(t, err, "signature verification failed")
}

func TestReceiptRejectsInvalidAuthenticatedClaims(t *testing.T) {
	t.Parallel()
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	for _, mutate := range []func(*httpreceipt.Claims){
		func(c *httpreceipt.Claims) { c.Version = "v2" },
		func(c *httpreceipt.Claims) { c.Replayed = 2 },
		func(c *httpreceipt.Claims) { c.Applies = 0 },
		func(c *httpreceipt.Claims) { c.TargetHash = "bad" },
		func(c *httpreceipt.Claims) { c.Status = 0 },
	} {
		claims := httpreceipt.Claims{Version: "v1", Method: "POST", TargetHash: digest("/"), BodyHash: digest(""), Status: 200, Applies: 1, Logs: 1, SequencesHash: digest("sequence")}
		mutate(&claims)
		_, err := httpreceipt.Sign(key, claims)
		require.Error(t, err)
		payload, err := json.Marshal(claims)
		require.NoError(t, err)
		raw, err := json.Marshal(map[string]any{"payload": payload, "signature": ed25519.Sign(key, append([]byte("formance.http-public-receipt.v1\x00"), payload...))})
		require.NoError(t, err)
		_, err = httpreceipt.Verify(base64.RawURLEncoding.EncodeToString(raw), key.Public().(ed25519.PublicKey), "POST", "/", 200, nil)
		require.Error(t, err)
	}
}

func TestReceiptAuthenticatesResponseWithoutApply(t *testing.T) {
	t.Parallel()
	key := ed25519.NewKeyFromSeed(make([]byte, 32))
	claims := httpreceipt.Claims{Version: "v1", Method: "POST", TargetHash: digest("/"), BodyHash: digest("{}"), Status: 400, SequencesHash: digest("")}
	header, err := httpreceipt.Sign(key, claims)
	require.NoError(t, err)
	actual, err := httpreceipt.Verify(header, key.Public().(ed25519.PublicKey), "POST", "/", 400, []byte("{}"))
	require.NoError(t, err)
	require.Equal(t, claims, *actual)
}
