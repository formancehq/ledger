package httpreceipt

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestVerifyBindsPublicApplyExchange(t *testing.T) {
	t.Parallel()
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	method, target, status, body := "POST", "/v3/_/apply", 200, []byte(`{"data":{"logs":[]}}`)
	targetHash, bodyHash := sha256.Sum256([]byte(target)), sha256.Sum256(body)
	header, err := Sign(key, Claims{Version: Version, Method: method, TargetHash: hex.EncodeToString(targetHash[:]), BodyHash: hex.EncodeToString(bodyHash[:]), Status: status})
	require.NoError(t, err)
	_, err = Verify(header, key.Public().(ed25519.PublicKey), method, target, status, body)
	require.NoError(t, err)
	otherSeed := make([]byte, ed25519.SeedSize)
	otherSeed[0] = 1
	_, err = Verify(header, ed25519.NewKeyFromSeed(otherSeed).Public().(ed25519.PublicKey), method, target, status, body)
	require.ErrorContains(t, err, "signature verification failed")
	for _, changed := range []struct {
		method string
		target string
		status int
		body   []byte
	}{
		{"GET", target, status, body},
		{method, "/v3/_/apply?x=1", status, body},
		{method, target, 201, body},
		{method, target, status, []byte(`{"data":{"logs":[1]}}`)},
	} {
		_, err = Verify(header, key.Public().(ed25519.PublicKey), changed.method, changed.target, changed.status, changed.body)
		require.Error(t, err)
	}
}

func TestVerifyRequiresReceiptDomain(t *testing.T) {
	t.Parallel()
	key := ed25519.NewKeyFromSeed(make([]byte, ed25519.SeedSize))
	body := []byte(`{"data":{"logs":[]}}`)
	targetHash, bodyHash := sha256.Sum256([]byte("/v3/_/apply")), sha256.Sum256(body)
	payload, err := json.Marshal(Claims{Version: Version, Method: http.MethodPost, TargetHash: hex.EncodeToString(targetHash[:]), BodyHash: hex.EncodeToString(bodyHash[:]), Status: http.StatusOK})
	require.NoError(t, err)
	for _, test := range []struct {
		name    string
		message []byte
		valid   bool
	}{
		{"receipt domain", append([]byte("formance.http-public-receipt.v1\x00"), payload...), true},
		{"bare payload", payload, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			raw, err := json.Marshal(envelope{Payload: payload, Signature: ed25519.Sign(key, test.message)})
			require.NoError(t, err)
			_, err = Verify(base64.RawURLEncoding.EncodeToString(raw), key.Public().(ed25519.PublicKey), http.MethodPost, "/v3/_/apply", http.StatusOK, body)
			if test.valid {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, "signature verification failed")
			}
		})
	}
}
