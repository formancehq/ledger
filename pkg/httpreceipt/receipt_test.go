package httpreceipt

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
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
