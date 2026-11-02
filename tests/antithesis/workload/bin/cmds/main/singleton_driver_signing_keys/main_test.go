package main

import (
	"crypto/ed25519"
	"testing"

	"github.com/stretchr/testify/require"
	"hegel.dev/go/hegel"

	"github.com/formancehq/ledger/v3/tests/antithesis/workload/internal/generation"
)

func TestGeneratedSigningKeyCoherence(t *testing.T) {
	t.Parallel()
	hegel.Test(t, func(tc *hegel.T) {
		key := generatedSigningKey(generation.NewSource(tc))
		require.Len(tc, key.Seed(), ed25519.SeedSize)
		require.Equal(tc, ed25519.NewKeyFromSeed(key.Seed()), key)
		payload := []byte("revoke generated signing key")
		require.True(tc, ed25519.Verify(key.Public().(ed25519.PublicKey), payload, ed25519.Sign(key, payload)))
	}, hegel.WithProfile("base"), hegel.WithBackend(hegel.BackendDefault), hegel.WithDatabase(""), hegel.WithSeed(42), hegel.WithTestCases(32), hegel.WithPhases(hegel.PhaseGenerate))
}
